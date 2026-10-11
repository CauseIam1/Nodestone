"""
Lodestone Walker Core — Phase 2 orchestration.

Blueprint §3 walk strategy (live-corrected, forward-first): account_tx pages
OLDEST-first (ledger_index_min=1, forward=true) so the genesis cluster
(account creation, first issuance, day-zero AMM) comes off the FIRST pages.
Live 2026-10-10 probes showed whitelisted issuer accounts carry 10^5-10^7 txs
of recent swap noise — backward paging would exhaust any budget before ever
reaching genesis, so forward-first is the ledger-truth-efficient direction.
Status semantics (budget-bounded Write-Once-Cache-Forever, lodestone.md §1):
    marker exhausted                     -> COMPLETE
    page budget tripped, genesis found   -> GENESIS_ONLY (resumable w/ new budget)
    page budget tripped, no genesis      -> PARTIAL
    issuer absent -> NO_ACCOUNT ; transport failure -> ERROR
Creator confidence (deterministic in-walk evidence; Phase 3 refines upstream):
    0.10 base
    +0.35 ghost wallet identified (Payment source that created the AccountRoot)
    +0.15 first issuance within ~1h of the genesis ledger
    +0.15 ghost re-engagement (AMM actor or genesis-issuance destination)
    +0.15 day-zero AMM created BY the ghost (+0.05 if any AMM exists)  cap 0.95
Writes (Isolation Rule): forensic_audit_events + token_lineage ONLY. Stdlib only.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from . import codenorm, xrpl_client
from . import filter as flt
from . import meta as mta
from . import qdb as qdb_mod
from .config import WalkerConfig

STATUS_COMPLETE = "COMPLETE"
STATUS_GENESIS_ONLY = "GENESIS_ONLY"
STATUS_PARTIAL = "PARTIAL"
STATUS_NO_ACCOUNT = "NO_ACCOUNT"
STATUS_ERROR = "ERROR"
STATUS_SKIPPED = "SKIPPED"


@dataclass
class WalkResult:
    currency: str
    issuer: str
    status: str = STATUS_ERROR
    ghost_wallet: Optional[str] = None
    amm_account: Optional[str] = None
    amm_create_ledger: Optional[int] = None
    confidence: float = 0.10
    genesis_ledger: Optional[int] = None
    genesis_tx_hash: Optional[str] = None
    pages: int = 0
    txs_scanned: int = 0
    hits_written: int = 0
    capped: bool = False
    notes: str = ""
    events_by_type: Dict[str, int] = field(default_factory=dict)


def _log_default(line: str):
    print(f"[lodestone {time.strftime('%H:%M:%S', time.gmtime())}] {line}")


def _topk_min(kept: List, item, cap: int, key: Callable):
    """Keep the `cap` smallest-key items — genesis hunting wants the earliest N."""
    if cap <= 0:
        return
    if len(kept) < cap:
        kept.append(item)
        return
    if key(item) >= max(key(x) for x in kept):
        return
    j = max(range(len(kept)), key=lambda i: key(kept[i]))
    kept[j] = item


def _sql_str(v: str) -> str:
    return str(v or "").replace("'", "''")


def _dedupe(hits: List) -> List:
    seen, out = set(), []
    for h in sorted(hits, key=lambda h: (h.ledger_index, h.tx_type, h.actor_address)):
        k = h.tx_hash or f"nohash@L{h.ledger_index}:{h.tx_type}:{h.actor_address}"
        if k in seen:
            continue
        seen.add(k)
        out.append(h)
    return out


def _score(ghost, genesis_l, first_iss_l, ghost_dests, amm_actor, amm_found, cfg):
    """Documented deterministic confidence model (see module header)."""
    s = 0.10
    if not ghost:
        return 0.10
    s += 0.35
    if genesis_l is not None and first_iss_l is not None and \
            first_iss_l - genesis_l <= cfg.genesis_issuance_window_ledgers:
        s += 0.15
    if ghost in (ghost_dests or ()) or (amm_actor and ghost == amm_actor):
        s += 0.15
    if amm_found:
        s += 0.15 if ghost == amm_actor else 0.05
    return round(min(0.95, s), 2)

def walk_token(currency, issuer, cfg=None, rpc=None, q=None, *,
               log: Callable[[str], None] = _log_default, dry_run: bool = False,
               force: bool = False, max_pages: Optional[int] = None) -> WalkResult:
    """One-token forensic walk -> audit events + lineage row (blueprint §3-§4).

    Raises ValueError for invalid walker input (CLI maps that to exit 1);
    transport/DB failures resolve into status=ERROR WalkResults instead.
    """
    cfg = cfg or WalkerConfig.from_env()
    if max_pages is None:
        max_pages = cfg.max_pages
    issuer = codenorm.normalize_issuer(issuer)
    disp = str(currency)
    hex40 = codenorm.currency_display_to_hex(disp)
    res = WalkResult(currency=disp, issuer=issuer)
    log(f"[·] walk start: {disp} / {issuer}")
    try:
        if rpc is None:
            rpc = xrpl_client.XrplClient(cfg.rpc_endpoints, cfg.rpc_timeout_s,
                                         cfg.rpc_rate_interval_s, cfg.rpc_max_retries,
                                         cfg.endpoint_down_cooldown_s)
        ad = ((rpc.account_info(issuer) or {}).get("account_data") or {})
        if not ad:
            res.status, res.notes = STATUS_NO_ACCOUNT, "issuer not found on any endpoint"
            log(f"[x] {res.notes}")
            return res
        log(u"[✓] issuer online: Sequence=%s OwnerCount=%s Flags=%s"
            % (ad.get("Sequence"), ad.get("OwnerCount"), ad.get("Flags")))
        if q is None and not dry_run:
            q = qdb_mod.QuestDbClient(cfg.qdb_exec_url, cfg.qdb_ilp_url,
                                      cfg.qdb_timeout_s, cfg.qdb_max_retries)
        existing = set()
        if q is not None:
            done = q.scalar("select count() from token_lineage where issuer='%s' "
                            "and currency='%s' and lineage_status='COMPLETE'"
                            % (_sql_str(issuer), _sql_str(disp)), default=0)
            if (done or 0) > 0 and not force:
                res.status = STATUS_SKIPPED
                res.notes = "COMPLETE lineage already cached (§1 write-once); --force re-walks"
                log(f"[✓] skip — {res.notes}")
                return res
            rows_ = q.dataset("select tx_hash from forensic_audit_events where "
                              "issuer='%s' and currency='%s' limit 100000"
                              % (_sql_str(issuer), _sql_str(disp)))
            for r_ in rows_:
                existing.add(str(r_[0]))

        # forward walk: oldest pages first, single pass, no unbounded retention
        pager = rpc.account_tx_pager(issuer, page_size=cfg.account_tx_page_size,
                                     max_pages=max_pages)
        genesis = None
        trustsets, issuances, amms, sets_, genesis_hits = [], [], [], [], []
        type_counts, total = {}, 0
        for page in pager.pages():
            if pager.pages_count == 1 or pager.pages_count % 10 == 0:
                log(f"[·] forward page {pager.pages_count} …")
            for entry in page:
                tx, md = mta.entry_tx(entry), mta.entry_meta(entry)
                if not tx:
                    continue
                lx = mta.entry_ledger_index(entry)
                if not lx or lx <= 0:
                    continue
                ttype = flt.tx_type_of(tx)
                type_counts[ttype] = type_counts.get(ttype, 0) + 1
                total += 1
                if genesis is None or lx < genesis[0]:
                    genesis = (lx, mta.entry_hash(entry), ttype,
                               str(tx.get("Account") or ""),
                               issuer in mta.created_account_roots(md))
                hit = flt.classify_entry(entry, currency=disp, currency_hex40=hex40,
                                         issuer=issuer)
                if hit is None:
                    continue
                cls = hit.payload.get("class")
                if hit.tx_type == "AMMCreate":
                    amms.append(hit)
                elif cls == "genesis_funding":
                    genesis_hits.append(hit)  # never capped — THE ghost anchor row
                elif cls == "trustline":
                    _topk_min(trustsets, hit, cfg.trustset_capture_limit,
                              lambda h: h.ledger_index)
                elif cls in ("token_issuance", "token_return"):
                    _topk_min(issuances, hit, cfg.issuance_capture_limit,
                              lambda h: h.ledger_index)
                elif cls == "issuer_config":
                    _topk_min(sets_, hit, cfg.accountset_capture_limit,
                              lambda h: h.ledger_index)
        res.pages, res.txs_scanned, res.capped = pager.pages_count, total, pager.capped
        log(u"[✓] history consumed: pages=%d txs=%d capped=%s"
            % (res.pages, total, res.capped))

        hits = _dedupe(trustsets + issuances + amms + sets_ + genesis_hits)
        for h in hits:
            if h.ripple_date is None:
                h.ripple_date = rpc.ledger_close_ripple_date(h.ledger_index)
                if h.ripple_date is None:
                    log(f"[!] no close time @L{h.ledger_index} — event skipped")
        amm_hit = min(amms, key=lambda h: h.ledger_index) if amms else None
        res.amm_account = amm_hit.payload.get("amm_account") if amm_hit else None
        res.amm_create_ledger = amm_hit.ledger_index if amm_hit else None
        amm_actor = amm_hit.actor_address if amm_hit else None
        iss_only = [h for h in hits if h.payload.get("class") == "token_issuance"]
        first_iss = min(iss_only, key=lambda h: h.ledger_index) if iss_only else None
        if genesis and genesis[4]:
            res.genesis_ledger, res.genesis_tx_hash = genesis[0], genesis[1]
            if genesis[3] not in ("", issuer):
                res.ghost_wallet = genesis[3]
        res.confidence = _score(res.ghost_wallet, res.genesis_ledger,
                                first_iss.ledger_index if first_iss else None,
                                {h.payload.get("destination") for h in iss_only},
                                amm_actor, bool(amm_hit), cfg)
        if res.capped:
            res.status = (STATUS_GENESIS_ONLY if res.genesis_ledger is not None
                          else STATUS_PARTIAL)
        elif res.genesis_ledger is not None:
            res.status = STATUS_COMPLETE
        else:
            res.status = STATUS_PARTIAL

        parts = []
        if genesis:
            parts.append(f"{'genesis' if genesis[4] else 'oldest'} {genesis[2]}"
                         f"@L{genesis[0]} via {genesis[3] or 'unknown'}")
        if first_iss:
            parts.append(f"first_issuance L{first_iss.ledger_index}")
        if amm_hit:
            parts.append(f"dayzero_amm {res.amm_account}@L{res.amm_create_ledger}"
                         f" by {amm_actor}")
        parts.append(f"kept trustlines={len(trustsets)} issuances={len(issuances)}"
                     f" amms={len(amms)} configs={len(sets_)}")
        parts.append(f"walk pages={res.pages} txs={total}"
                     + (" FORWARD-CAPPED" if res.capped else " full"))
        res.notes = " | ".join(parts)
        for h in hits:
            res.events_by_type[h.tx_type] = res.events_by_type.get(h.tx_type, 0) + 1
        log(u"[✓] origin story: ghost=%s amm=%s@%s conf=%.2f status=%s"
            % (res.ghost_wallet, res.amm_account, res.amm_create_ledger,
               res.confidence, res.status))

        if dry_run:
            res.hits_written = sum(1 for h in hits if h.ripple_date)
            log(f"[✓] dry-run: would write {res.hits_written} audit events"
                " + 1 lineage row")
            return res
        if q is None:
            raise RuntimeError("qdb client unavailable")
        now_unix = int(time.time())
        lines = []
        for h in hits:
            if not h.ripple_date or (h.tx_hash and h.tx_hash in existing):
                continue
            lines.append(qdb_mod.audit_event_line(
                codenorm.ripple_date_to_unix(h.ripple_date), disp, issuer,
                h.tx_hash or f"nohash@L{h.ledger_index}", h.tx_type,
                h.actor_address, h.ledger_index, mta.compact_json(h.payload)))
        res.hits_written = len(lines)
        if lines:
            q.write_lines(lines)
            log(f"[✓] committed {len(lines)} events -> forensic_audit_events")
        else:
            log("[·] 0 new audit events (dedupe/empty)")
        q.write_lines([qdb_mod.lineage_line(
            now_unix, disp, issuer, res.ghost_wallet or "", res.amm_account or "",
            res.amm_create_ledger, res.confidence, res.status, res.notes)])
        expected = res.hits_written
        for _ in range(4):  # WAL drain tolerance loop (graceful QuestDB handling)
            ev_count = int(q.scalar("select count() from forensic_audit_events where "
                                    "issuer='%s' and currency='%s'"
                                    % (_sql_str(issuer), _sql_str(disp)),
                                    default=0) or 0)
            if ev_count >= expected:
                break
            time.sleep(2)
        log(f"[✓] verified: forensic_audit_events rows for token = {ev_count}"
            f" (wrote {expected})")
        return res
    except xrpl_client.RpcError as e:
        res.status, res.notes = STATUS_ERROR, f"rpc: {e}"
        log(f"[x] walk failed: {res.notes}")
        return res
    except qdb_mod.QdbError as e:
        res.status, res.notes = STATUS_ERROR, f"qdb: {e}"
        log(f"[x] walk failed: {res.notes}")
        return res
    except Exception as e:  # defensive last resort — never crash the CLI loop
        res.status, res.notes = STATUS_ERROR, f"{type(e).__name__}: {e}"
        log(f"[x] walk failed: {res.notes}")
        return res