"""
Lodestone — XRPL JSON-RPC transport (stdlib; blueprint §3).

§3 Remote Full-History Fallback: endpoint-by-endpoint w/ health cooldowns so
historical traffic lands on full-history archive nodes. §3 Pagination: account_tx
paged via `marker` until None — COMPLETE history; forward=True pages OLDEST-first
(live-probed: issuers carry 10^5-10^7 noise txs; backward paging never reaches
genesis). Transient errors back off + fail over; deterministic errors raise fast.
"""
import json
import time
import urllib.error
import urllib.request
from typing import Iterator

_TRANSIENT_JSON_ERRORS = (
    "tooBusy", "rateLimit", "highLoadFactor", "serverBusy", "slowDown", "noPermission",
)
_TRANSIENT_HTTP = {425, 429, 500, 502, 503, 504}


class RpcError(RuntimeError):
    """Fatal-after-retries JSON-RPC/transport failure (transient flag for logs)."""

    def __init__(self, message: str, *, endpoint=None, transient=False, http_code=None):
        super().__init__(message)
        self.endpoint = endpoint
        self.transient = transient
        self.http_code = http_code


class XrplClient:
    def __init__(self, endpoints=(), timeout_s: float = 20.0,
                 rate_interval_s: float = 0.3, max_retries: int = 2,
                 cooldown_s: float = 120.0):
        self.endpoints = tuple(endpoints) or ("https://xrplcluster.com",)
        self.timeout_s = float(timeout_s)
        self.rate_interval_s = float(rate_interval_s)
        self.max_retries = int(max_retries)
        self.cooldown_s = float(cooldown_s)
        self._down_until = {}      # endpoint -> monotonic time it turns hot again
        self._last_call = 0.0

    def _healthy_endpoints(self):
        now = time.monotonic()
        live = [e for e in self.endpoints if self._down_until.get(e, 0.0) <= now]
        return live or list(self.endpoints)  # emergency: all cooling -> try all

    def call(self, method: str, **args) -> dict:
        """POST one JSON-RPC call with pacing, retries and archive failover."""
        args = {k: v for k, v in args.items() if v is not None}
        body = json.dumps({"method": method, "params": [args]}).encode("utf-8")
        last_exc = None
        for _attempt in range(self.max_retries + 1):
            for ep in self._healthy_endpoints():
                gap = self.rate_interval_s - (time.monotonic() - self._last_call)
                if gap > 0:
                    time.sleep(gap)
                self._last_call = time.monotonic()
                req = urllib.request.Request(
                    ep, data=body, method="POST",
                    headers={"Content-Type": "application/json",
                             "User-Agent": "LodestoneWalker/0.1"})
                try:
                    with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                        raw = resp.read()
                    self._down_until.pop(ep, None)
                    resp_obj = json.loads(raw)
                    if resp_obj.get("error") not in (None, ""):
                        raise RpcError(f"{method}: top-level error {resp_obj.get('error')!r}",
                                       endpoint=ep)
                    result = resp_obj.get("result") or {}
                    jerr = result.get("error")
                    if jerr in (None, ""):
                        return result
                    transient = any(k in str(jerr) for k in _TRANSIENT_JSON_ERRORS)
                    raise RpcError(
                        f"{method}: {jerr} {result.get('error_message') or ''}".rstrip(),
                        endpoint=ep, transient=transient)
                except RpcError as e:
                    if e.transient:
                        last_exc = e
                        self._down_until[ep] = time.monotonic() + self.cooldown_s
                        continue
                    raise  # deterministic (accountNotFound etc.) — fail fast
                except urllib.error.HTTPError as e:
                    last_exc = RpcError(f"{method}: HTTP {e.code} from {ep}",
                                        endpoint=ep, http_code=e.code)
                    self._down_until[ep] = time.monotonic() + self.cooldown_s
                except (urllib.error.URLError, TimeoutError, json.JSONDecodeError,
                        OSError) as e:
                    last_exc = e
                    self._down_until[ep] = time.monotonic() + self.cooldown_s
        raise RpcError(f"{method}: exhausted {len(self.endpoints)} endpoint(s) after "
                       f"{self.max_retries + 1} cycle(s) "
                       f"({type(last_exc).__name__}: {last_exc})")

    def account_info(self, account: str) -> dict:
        return self.call("account_info", account=account)

    def account_tx_pager(self, account: str, *, page_size: int = 200,
                         min_ledger: int = 1, max_ledger: int = -1,
                         max_pages: int = 4000, forward: bool = True) -> "AccountTxPager":
        return AccountTxPager(self, account, page_size, min_ledger, max_ledger,
                              max_pages, forward)

    def ledger_close_ripple_date(self, ledger_index: int):
        """Ledger close time (ripple epoch), per-instance cache; None on error."""
        cache = getattr(self, "_ledger_dates", None)
        if cache is None:
            cache = self._ledger_dates = {}
        if ledger_index in cache:
            return cache[ledger_index]
        try:
            res = self.call("ledger", ledger_index=ledger_index)
            d = (res.get("ledger") or {}).get("close_time")
            cache[ledger_index] = int(d) if d is not None else None
        except RpcError:
            cache[ledger_index] = None
        return cache[ledger_index]


class AccountTxPager:
    """Marker-driven full account_tx pagination (default: oldest -> newest)."""

    def __init__(self, client: XrplClient, account: str, page_size: int,
                 min_ledger: int, max_ledger: int, max_pages: int, forward: bool):
        self.client = client
        self.account = account
        self.page_size = max(1, int(page_size))
        self.min_ledger = int(min_ledger)
        self.max_ledger = int(max_ledger)
        self.max_pages = int(max_pages)
        self.forward = bool(forward)
        self.pages_count = 0
        self.capped = False

    def pages(self) -> Iterator[list]:
        """Yields raw page lists; stops when marker=None or the page cap trips.

        `max_ledger=-1` + `min_ledger=1` covers the account from inception; the
        XRPL marker carries pagination state across calls exactly as blueprint
        §3 specifies.
        """
        marker = None
        while True:
            res = self.client.call(
                "account_tx", account=self.account,
                ledger_index_min=self.min_ledger, ledger_index_max=self.max_ledger,
                limit=self.page_size, forward=self.forward,
                **({"marker": marker} if marker else {}))
            self.pages_count += 1
            page = list(res.get("transactions") or [])
            yield page
            marker = res.get("marker")
            if marker is None:
                return
            if self.pages_count >= self.max_pages:
                self.capped = True
                return

    def entries(self) -> Iterator[dict]:
        for page in self.pages():
            for entry in page:
                yield entry