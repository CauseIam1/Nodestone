"""
Lodestone — QuestDB HTTP bridge (stdlib; Phase 1 schema contract).

Schema contract: Lodestone/lodestone_init.sql == questdb/init/04-lodestone-
lineage-schema.sql (Phase 1). The ONLY tables ever addressed here:
    token_lineage          — resolved origin story rows (designated discovered_at)
    forensic_audit_events  — granular walk hits (designated timestamp)
Isolation Rule: no other table is ever read or written here.

Transport (live-probed 2026-10-10):
  * Writes: ILP over HTTP  POST /write?precision=ns -> HTTP 204. One POST per
    walk batch; duplicate identical rows (same ns + key) MERGE, which makes a
    retry after a timeout-but-applied request non-duplicating.
  * Reads / verification: GET /exec with URL-encoded SQL (Phase 1 proven path).

Stdlib only.
"""
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import List, Optional


class QdbError(RuntimeError):
    pass


def sql_str(v) -> str:
    """QuestDB single-quote escaping for WHERE-clause literals."""
    return str(v or "").replace("'", "''")


def _esc_tag(v) -> str:
    """ILP symbol/tag escape: backslash, space, comma (QuestDB ILP grammar)."""
    s = "" if v is None else str(v)
    return s.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


def ilp_string(v) -> str:
    """ILP STRING field: double-quoted, backslash+quote escaped, controls dropped."""
    s = "" if v is None else str(v)
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    s = re.sub(r"[\x00-\x1f\x7f]", " ", s)
    return '"' + s + '"'


def ilp_long(v) -> str:
    return f"{int(v)}i"


def ilp_float(v) -> str:
    f = float(v)
    if f != f or f in (float("inf"), float("-inf")):
        raise QdbError(f"non-finite numeric field: {v!r}")
    return repr(f)


def audit_event_line(ts_unix: int, currency: str, issuer: str, tx_hash: str,
                     tx_type: str, actor_address: str, ledger_index: int,
                     payload_json: str) -> str:
    """One forensic_audit_events ILP line (timestamp column = ledger close time)."""
    tags = (
        f"forensic_audit_events,currency={_esc_tag(currency)},"
        f"issuer={_esc_tag(issuer)},tx_hash={_esc_tag(tx_hash)},"
        f"tx_type={_esc_tag(tx_type)},actor_address={_esc_tag(actor_address)}"
    )
    fields = (f"ledger_index={ilp_long(ledger_index)},"
              f"payload_json={ilp_string(payload_json)}")
    return f"{tags} {fields} {int(ts_unix) * 1_000_000_000}"


def lineage_line(ts_unix: int, currency: str, issuer: str, funding_wallet: str,
                 amm_account: str, amm_create_ledger: Optional[int],
                 creator_confidence: float, lineage_status: str,
                 notes: str) -> str:
    """One token_lineage ILP line (discovered_at = walk commit time).

    Multiple rows per token are legal history; readers use the QuestDB 8.x
    state-collapse grammar documented in the schema headers:
    `LATEST ON discovered_at PARTITION BY issuer, currency`.
    """
    tags = (
        f"token_lineage,currency={_esc_tag(currency)},issuer={_esc_tag(issuer)},"
        f"funding_wallet={_esc_tag(funding_wallet)},"
        f"amm_account={_esc_tag(amm_account)},"
        f"lineage_status={_esc_tag(lineage_status)}"
    )
    fields = []
    if amm_create_ledger is not None:
        fields.append(f"amm_create_ledger={ilp_long(amm_create_ledger)}")
    fields.append(f"creator_confidence={ilp_float(creator_confidence)}")
    fields.append(f"notes={ilp_string(notes)}")
    return f"{tags} {','.join(fields)} {int(ts_unix) * 1_000_000_000}"

# ---------------------------------------------------------------------------

class QuestDbClient:
    def __init__(self, exec_url: str, ilp_url: str, timeout_s: float = 10.0,
                 max_retries: int = 3, backoff_s: float = 1.5):
        if not exec_url or not ilp_url:
            raise QdbError("QuestDbClient requires exec_url and ilp_url")
        self.exec_url = exec_url
        self.ilp_url = ilp_url
        self.timeout_s = float(timeout_s)
        self.max_retries = int(max_retries)
        self.backoff_s = float(backoff_s)

    def exec_sql(self, query: str) -> dict:
        """GET /exec (URL-encoded SQL); deterministic errors raise at once."""
        url = self.exec_url + ("&" if "?" in self.exec_url else "?") + \
            urllib.parse.urlencode({"query": query})
        last = None
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(url, timeout=self.timeout_s) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode("utf-8", "replace")[:400]
                except Exception:
                    pass
                last = QdbError(f"/exec HTTP {e.code}: {body or e.reason}")
                if e.code not in (429, 500, 502, 503, 504):
                    raise last  # deterministic query error — no retry loop
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = QdbError(f"/exec transport failure: {e}")
            if attempt < self.max_retries:
                time.sleep(self.backoff_s * (attempt + 1))
        raise last or QdbError("/exec failed")

    def scalar(self, query: str, default=None):
        """First dataset value of a single-column query (e.g. count())."""
        try:
            data = self.exec_sql(query)
        except QdbError:
            return default
        ds = data.get("dataset") or []
        return ds[0][0] if ds else default

    def dataset(self, query: str) -> list:
        return self.exec_sql(query).get("dataset") or []

    def write_lines(self, lines: List[str]):
        """One batched ILP POST; 204 == accepted (QuestDB ILP/HTTP, live-probed).

        Retry-safety: ILP merge semantics make an identical re-POST
        non-duplicating — a timeout-but-applied batch is safe to resend.
        """
        if not lines:
            return
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        last = None
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    self.ilp_url, data=payload, method="POST",
                    headers={"Content-Type": "text/plain",
                             "User-Agent": "LodestoneWalker/0.1"})
                with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                    code = r.status
                if code == 204:
                    return
                raise QdbError(f"/write unexpected status {code}")
            except urllib.error.HTTPError as e:
                body = ""
                try:
                    body = e.read().decode("utf-8", "replace")[:400]
                except Exception:
                    pass
                last = QdbError(f"/write HTTP {e.code}: {body or e.reason}")
                if e.code not in (429, 500, 502, 503, 504):
                    raise last  # malformed line — fix data, don't retry
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = QdbError(f"/write transport failure: {e}")
            if attempt < self.max_retries:
                time.sleep(self.backoff_s * (attempt + 1))
        raise last or QdbError("/write failed")