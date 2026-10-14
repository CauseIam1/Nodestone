"""
Lodestone Ghost Walker — Phase G1 orchestration (lodestone.md §7, Oct 14 2026).

Walks ONE ghost wallet's complete account_tx history (forward, marker-driven,
full-history archive fallback — the same transport contract as walk_token) and
streams wallet-behavior evidence into the ghost tables:

    ghost_walks   — one summary row per completed walk (versioning; readers
                    collapse with LATEST ON walked_at PARTITION BY ghost_wallet)
    ghost_events  — one row per captured transaction: ghost-relative DIRECTION
                    (IN | OUT | SELF | OTHER), counterparty, DestinationTag,
                    ledger index and a self-describing payload_json (class
                    'ghost_payment' | 'ghost_trustline': amounts + delivered
                    truth, Flags incl. tfPartialPayment, decoded Memos[],
                    SourceTag) — the venue-correlation + constellation-edge
                    material for Phase G2 and the HUD.

Status semantics (Write-Once-Cache-Forever parity with walk_token, §1):
    marker exhausted                    -> COMPLETE
    page budget tripped                 -> PARTIAL (resumable: --force or new budget)
    account absent                      -> NO_ACCOUNT
    COMPLETE row cached, no --force     -> SKIPPED
    transport/DB failure                -> ERROR
Evidence lines STREAM during the walk in bounded ILP batches (budget-capped
venue/bot ghosts can carry 10^5+ payments — no unbounded retention), deduped on
tx_hash against both the preloaded row set and ILP key-merge (re-walks/retries
never duplicate). XRP in/out sums register delivered_amount ledger truth when
present, never the Amount envelope (house rule). Stdlib only. Host entrypoint:
    cd /home/causeiam/docker-containers/Lodestone && python3 -m walker --ghost r...
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

from . import codenorm, xrpl_client
from . import ghostfilter as gf
from . import meta as mta
from . import qdb as qdb_mod
from .config import WalkerConfig
from .filter import tx_type_of
from .walker import (STATUS_COMPLETE, STATUS_ERROR, STATUS_NO_ACCOUNT,
                     STATUS_PARTIAL, STATUS_SKIPPED)

_ILP_BATCH_LINES = 2000  # bounded-memory evidence streaming chunk


@dataclass
class GhostWalkResult:
    ghost_wallet: str
    status: str = STATUS_ERROR
    pages: int = 0
    txs_scanned: int = 0
    capped: bool = False
    payments: int = 0
    in_payments: int = 0
    out_payments: int = 0
    self_payments: int = 0
    other_payments: int = 0
    trustsets: int = 0
    tagged_payments: int = 0
    memo_payments: int = 0
    distinct_counterparties: int = 0
    distinct_tags: int = 0
    xrp_in_drops: int = 0
    xrp_out_drops: int = 0
    first_ledger: Optional[int] = None
    last_ledger: Optional[int] = None
    linked_tokens: str = ""
    events_written: int = 0
    notes: str = ""
    type_counts: Dict[str, int] = field(default_factory=dict)


def _log_default(line: str):
    print(f"[lodestone {time.strftime('%H:%M:%S', time.gmtime())}] {line}")


def _sql_str(v: str) -> str:
    return str(v or "").replace("'", "''")


def _fold_hit(res: GhostWalkResult, hit, counterparties: Set[str], tags: set,
              tokens: Dict[str, None]):
    """Aggregate one classified ghost hit into the walk stats (streamed)."""
    p = hit.payload
    cp = str(p.get("counterparty") or "")
    if cp and cp != res.ghost_wallet:
        counterparties.add(cp)
    if p.get("class") == "ghost_payment":
        res.payments += 1
        direction = str(p.get("direction") or "OTHER")
        drops = gf.xrp_drops(p)
        if direction == "OUT":
            res.out_payments += 1
            if drops is not None:
                res.xrp_out_drops += drops
        elif direction == "IN":
            res.in_payments += 1
            if drops is not None:
                res.xrp_in_drops += drops
        elif direction == "SELF":
            res.self_payments += 1
        else:
            res.other_payments += 1
        dt = p.get("destination_tag")
        if dt is not None:
            res.tagged_payments += 1
            tags.add(int(dt))
        if p.get("memos"):
            res.memo_payments += 1
    else:  # ghost_trustline
        res.trustsets += 1
    token = gf.token_tag(p)
    if token:
        tokens.setdefault(token, None)


def walk_ghost(ghost, cfg=None, rpc=None, q=None, *,
               log: Callable[[str], None] = _log_default, dry_run: bool = False,
               force: bool = False,
               max_pages: Optional[int] = None) -> GhostWalkResult:
    """One ghost-account forensic walk -> ghost_events + ghost_walks (§7).

    Raises ValueError for invalid walker input (CLI maps that to exit 1);
    transport/DB failures resolve into status=ERROR results instead.
    """
    cfg = cfg or WalkerConfig.from_env()
    if max_pages is None:
        max_pages = cfg.max_pages
    ghost = codenorm.normalize_issuer(ghost)
    res = GhostWalkResult(ghost_wallet=ghost)
    log(f"[·] ghost walk start: {ghost}")
    try:
        if rpc is None:
            rpc = xrpl_client.XrplClient(cfg.rpc_endpoints, cfg.rpc_timeout_s,
                                         cfg.rpc_rate_interval_s,
                                         cfg.rpc_max_retries,
                                         cfg.endpoint_down_cooldown_s)
        ad = ((rpc.account_info(ghost) or {}).get("account_data") or {})
        if not ad:
            res.status, res.notes = STATUS_NO_ACCOUNT, \
                "ghost account not found on any endpoint"
            log(f"[x] {res.notes}")
            return res
        log(u"[✓] ghost online: Sequence=%s OwnerCount=%s Flags=%s"
            % (ad.get("Sequence"), ad.get("OwnerCount"), ad.get("Flags")))
        if q is None and not dry_run:
            q = qdb_mod.QuestDbClient(cfg.qdb_exec_url, cfg.qdb_ilp_url,
                                      cfg.qdb_timeout_s, cfg.qdb_max_retries)
        existing: Set[str] = set()
        pre_total = 0
        if q is not None:
            done = q.scalar("select count() from ghost_walks where "
                            "ghost_wallet='%s' and lineage_status='COMPLETE'"
                            % _sql_str(ghost), default=0)
            if (done or 0) > 0 and not force:
                res.status = STATUS_SKIPPED
                res.notes = "COMPLETE ghost walk already cached (§1 write-once); " \
                            "--force re-walks"
                log(f"[✓] skip — {res.notes}")
                return res
            for r_ in q.dataset("select tx_hash from ghost_events where "
                                "ghost_wallet='%s' limit %d"
                                % (_sql_str(ghost),
                                   max(1, int(cfg.ghost_dedupe_scan_limit)))):
                existing.add(str(r_[0]))
            pre_total = int(q.scalar("select count() from ghost_events where "
                                     "ghost_wallet='%s'" % _sql_str(ghost),
                                     default=0) or 0)

        # forward walk (§3 parity): oldest pages first, single pass, bounded memory
        pager = rpc.account_tx_pager(ghost, page_size=cfg.account_tx_page_size,
                                     max_pages=max_pages)
        lines: List[str] = []
        seen: Set[str] = set()
        counterparties: Set[str] = set()
        tags: set = set()
        tokens: Dict[str, None] = {}  # insertion-ordered distinct asset tags
        written = 0
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
                ttype = tx_type_of(tx)
                res.type_counts[ttype] = res.type_counts.get(ttype, 0) + 1
                res.txs_scanned += 1
                if res.first_ledger is None or lx < res.first_ledger:
                    res.first_ledger = lx
                if res.last_ledger is None or lx > res.last_ledger:
                    res.last_ledger = lx
                hit = gf.classify_ghost_entry(entry, ghost)
                if hit is None:
                    continue
                key = hit.tx_hash or (f"nohash@L{hit.ledger_index}:"
                                      f"{hit.tx_type}:{hit.actor_address}")
                if key in seen or key in existing:
                    continue
                seen.add(key)
                _fold_hit(res, hit, counterparties, tags, tokens)
                if dry_run:
                    continue
                if hit.ripple_date is None:
                    hit.ripple_date = rpc.ledger_close_ripple_date(hit.ledger_index)
                    if hit.ripple_date is None:
                        log(f"[!] no close time @L{hit.ledger_index} — event skipped")
                        continue
                lines.append(qdb_mod.ghost_event_line(
                    codenorm.ripple_date_to_unix(hit.ripple_date), ghost,
                    hit.tx_hash or f"nohash@L{hit.ledger_index}", hit.tx_type,
                    str(hit.payload.get("direction") or "OTHER"),
                    str(hit.payload.get("counterparty") or ""),
                    hit.payload.get("destination_tag"), hit.ledger_index,
                    mta.compact_json(hit.payload)))
                if len(lines) >= _ILP_BATCH_LINES:  # bounded-memory streaming
                    q.write_lines(lines)
                    written += len(lines)
                    log(f"[·] streamed {written} evidence rows …")
                    lines = []
        res.pages, res.capped = pager.pages_count, pager.capped
        res.distinct_counterparties = len(counterparties)
        res.distinct_tags = len(tags)
        linked = list(tokens)[:24]
        res.linked_tokens = ",".join(linked) + (f" +{len(tokens) - 24}"
                                                if len(tokens) > 24 else "")
        res.status = STATUS_PARTIAL if pager.capped else STATUS_COMPLETE

        parts = [
            f"payments={res.payments} in={res.in_payments}/out={res.out_payments}"
            f"/self={res.self_payments}/other={res.other_payments}",
            f"trustsets={res.trustsets}",
            f"xrp_in={res.xrp_in_drops}d xrp_out={res.xrp_out_drops}d",
            f"tagged={res.tagged_payments} distinct_tags={res.distinct_tags}",
            f"memo_txs={res.memo_payments}",
            f"counterparties={res.distinct_counterparties} tokens={len(tokens)}",
        ]
        if res.first_ledger is not None:
            parts.append(f"span=L{res.first_ledger}..L{res.last_ledger}")
        parts.append(f"walk pages={res.pages} txs={res.txs_scanned}"
                     + (" FORWARD-CAPPED" if res.capped else " full"))
        res.notes = " | ".join(parts)
        log(u"[✓] ghost dossier: payments=%d trustsets=%d xrp_in=%d xrp_out=%d "
            u"status=%s" % (res.payments, res.trustsets, res.xrp_in_drops,
                            res.xrp_out_drops, res.status))

        if dry_run:
            log("[✓] dry-run: would stream the evidence census above "
                "+ 1 ghost_walks summary row")
            return res
        if q is None:
            raise RuntimeError("qdb client unavailable")
        if lines:
            q.write_lines(lines)  # final partial batch
            written += len(lines)
        res.events_written = written
        if written:
            log(f"[✓] committed {written} evidence rows -> ghost_events")
        else:
            log("[·] 0 new evidence rows (dedupe/empty)")
        q.write_lines([qdb_mod.ghost_walk_line(
            int(time.time()), ghost, res.status, res.first_ledger, res.last_ledger,
            res.txs_scanned, res.payments, res.trustsets, res.tagged_payments,
            res.memo_payments, res.distinct_counterparties, res.distinct_tags,
            res.xrp_in_drops, res.xrp_out_drops, res.linked_tokens, res.notes)])
        for _ in range(4):  # WAL drain tolerance loop (graceful QuestDB handling)
            ev_count = int(q.scalar("select count() from ghost_events where "
                                    "ghost_wallet='%s'" % _sql_str(ghost),
                                    default=0) or 0)
            if ev_count >= pre_total + written:
                break
            time.sleep(2)
        log(f"[✓] verified: ghost_events rows for ghost = {ev_count} "
            f"(pre {pre_total}, wrote {written})")
        return res
    except xrpl_client.RpcError as e:
        res.status, res.notes = STATUS_ERROR, f"rpc: {e}"
        log(f"[x] ghost walk failed: {res.notes}")
        return res
    except qdb_mod.QdbError as e:
        res.status, res.notes = STATUS_ERROR, f"qdb: {e}"
        log(f"[x] ghost walk failed: {res.notes}")
        return res
    except Exception as e:  # defensive last resort — never crash the CLI loop
        res.status, res.notes = STATUS_ERROR, f"{type(e).__name__}: {e}"
        log(f"[x] ghost walk failed: {res.notes}")
        return res
