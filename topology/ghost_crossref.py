"""
Lodestone Ghost Venue Crossref — Phase G2 (lodestone.md §8, Oct 14 2026).

Correlates ALL walked ghost dossiers (ghost_events, §7) into per-ghost venue
attributions and same-exit clusters, derived from LEDGER EVIDENCE ONLY —
never hardcoded exchange address lists (§7 ruling). Three signal classes,
pure and deterministic:

  TAG        OUT Payments carrying DestinationTag to counterparty V are the
             venue deposit pattern (per-user deposit IDs, Binance-style);
             the SAME tag value reused at the SAME V by DIFFERENT ghosts is
             the strong same-exit fingerprint.
  MEMO       hex-decoded memo identities (the G1 lens decodes Memos[] from
             XRPL wire form) repeated across ghosts at one counterparty.
  ROUNDTRIP  ghost A OUT exactly N to relay X and ghost B (A != B enforced)
             IN exactly N from X within ghost_roundtrip_window_ledgers —
             XRP paired on delivered drop integers, IOUs on identical
             currency+issuer+value serialization; single-ghost loops stay
             notes-only, never venue evidence.

Amounts register delivered_amount ledger truth over the Amount envelope
(house rule). Venue rows commit to ghost_venues (append-only versioning;
readers collapse with LATEST ON attributed_at PARTITION BY ghost_wallet,
venue_address); venue_label is ledger-derived (memo identity | tag
fingerprint | relay), never a hardcoded venue name. Evidence rubric (§8,
deterministic, clamp 0.98 — absolute ledger proof stays reserved):
    0.30 base (any evidence) + 0.05 per extra tagged event (cap +0.20)
  + 0.15 cross-ghost tag reuse + 0.10 cross-ghost memo identity
  + 0.05 per round-trip leg (cap +0.10) + 0.05 same-exit cluster (>=2 ghosts)
Idempotence mirrors the §4 mapper: re-runs skip attributions whose census
notes are unchanged; --force re-attributes. Writes (Isolation Rule):
ghost_venues plus the Phase G3 VENUE constellation emission (Lodestone-owned:
one VENUE node + one VENUE_LINK edge per attributed (ghost, venue) pair,
appended into the ghost's own cluster). Reads: ghost_walks + ghost_events +
existing topology_nodes / topology_edges rows (all Lodestone-owned).

Phase G3 (lodestone.md §13) — venue constellation emission convention:
    cluster_id   the ghost's own GHOST-role cluster (C + sha256(ghost)[:12]);
    currency/issuer  '' — venue links are ACCOUNT-scoped, never token edges;
    edge_type    VENUE_LINK, edge_confidence = the §8 evidence rubric score;
    ledger_index/tx_hash  empty — the attribution is census-derived, never a
                 single-tx claim. LATEST-ON versioning + census-payload skip
                 compare keep re-runs idempotent; readers stay on the plain
                 §2 topology grammar.
Stdlib only. Host entrypoint:
    cd /home/causeiam/docker-containers/Lodestone && python3 -m topology --ghosts
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

from walker.config import WalkerConfig
from walker.qdb import QuestDbClient
from walker.ghostfilter import xrp_drops
from walker.walker import STATUS_COMPLETE, STATUS_PARTIAL
from .qdbx import (TopologyClient, ghost_venue_line, topology_edge_line,
                   topology_node_line)

_FP_CAP = 8            # memo fingerprints kept per venue row
_FP_TEXT_CAP = 80      # characters kept per memo fingerprint
_XREF_CAP = 24         # same-exit ghosts kept per row
_RT_BUCKET_CAP = 2000  # defensive window-match cap per (amount, relay) bucket
_SCORE_MAX = 0.98      # house rubric ceiling (§4/§8)


@dataclass
class VenueEvidence:
    """Accumulated §8 signals for ONE (ghost, venue counterparty) pair."""

    ghost: str
    venue: str
    tagged: List[Tuple[int, str, int]] = field(default_factory=list)
    #   ^ (destination_tag, tx_hash, ledger_index) of tagged OUT deposits
    memo_events: int = 0
    memo_fps: Dict[str, int] = field(default_factory=dict)
    #   ^ memo fingerprint -> event count
    roundtrips: List[Tuple[str, str]] = field(default_factory=list)
    #   ^ (partner_ghost, 'Lout-Lin') relay leg pairs


def _log(line: str):
    print("[lodestone-crossref %s] %s"
          % (time.strftime('%H:%M:%S', time.gmtime()), line))


def _event_view(row: dict) -> Optional[dict]:
    """One ghost_events row -> normalized correlation view; None when unusable
    (unparsable payload, missing counterparty or ledger index)."""
    try:
        p = json.loads(str(row.get("payload_json") or "{}"))
    except ValueError:
        return None
    if not isinstance(p, dict):
        return None
    try:
        li = (int(row.get("ledger_index"))
              if row.get("ledger_index") not in (None, "") else 0)
    except (TypeError, ValueError):
        li = 0
    return {"tx": str(row.get("tx_hash") or ""),
            "type": str(row.get("tx_type") or ""),
            "dir": str(row.get("direction") or ""),
            "cp": str(row.get("counterparty") or p.get("counterparty") or ""),
            "li": li, "p": p}


def _amount_key(p: dict) -> Optional[tuple]:
    """Exact-match asset amount of a ghost_payment payload — delivered_amount
    ledger truth first, envelope fallback (house rule). XRP pairs on
    delivered drop ints; IOUs on identical currency+issuer+value
    serialization."""
    n = xrp_drops(p)
    if n is not None and n > 0:
        return ("XRP", n)
    amt = p.get("delivered") if isinstance(p.get("delivered"), dict) else p
    if (isinstance(amt, dict) and isinstance(amt.get("currency"), str)
            and amt.get("currency") and amt.get("issuer")
            and amt.get("value") is not None):
        return ("IOU", str(amt["currency"]), str(amt["issuer"]),
                str(amt["value"]))
    return None


_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")  # matches ilp_string()'s sanitizer


def _field_text(f) -> str:
    """Decoded memo field -> bounded fingerprint text (text beats hex form).
    Control characters are normalized to spaces HERE so fingerprints/labels
    are byte-stable against their ILP-stored forms (a raw newline inside a
    memo would otherwise re-map its pair on every run — storage stores the
    sanitized text, the census would compare raw)."""
    if isinstance(f, str):
        return _CTRL_RE.sub(" ", f)[:_FP_TEXT_CAP]
    if isinstance(f, dict):
        if f.get("text"):
            return _CTRL_RE.sub(" ", str(f["text"]))[:_FP_TEXT_CAP]
        if f.get("hex"):
            return "0x" + str(f["hex"])[:_FP_TEXT_CAP]
    return ""


def memo_fingerprints(p: dict) -> List[str]:
    """Sorted stable memo fingerprints of one payload (deterministic order)."""
    out: Set[str] = set()
    for m in p.get("memos") or []:
        if not isinstance(m, dict):
            continue
        fp, tp = _field_text(m.get("data")), _field_text(m.get("type"))
        if fp:
            out.add("%s|%s" % (tp, fp) if tp else fp)
    return sorted(out)[:_FP_CAP]


def venue_evidence(events_by_ghost: Dict[str, List[dict]]
                   ) -> Dict[Tuple[str, str], VenueEvidence]:
    """§8 signal pass 1 — tagged-deposit + memo evidence per (ghost, venue).
    Only counterparties with at least one signal get a VenueEvidence."""
    out: Dict[Tuple[str, str], VenueEvidence] = {}
    for ghost, rows in events_by_ghost.items():
        for row in rows:
            ev = _event_view(row)
            if ev is None or not ev["cp"] or ev["li"] <= 0:
                continue
            key = (ghost, ev["cp"])
            ve = out.get(key)
            if ve is None:
                ve = out[key] = VenueEvidence(ghost, ev["cp"])
            if ev["type"] == "Payment" and ev["dir"] == "OUT":
                tag = ev["p"].get("destination_tag")
                if tag is None:
                    tag = row.get("destination_tag")
                try:
                    tag = int(tag) if tag is not None else None
                except (TypeError, ValueError):
                    tag = None
                if tag is not None and 0 <= tag <= 0xFFFFFFFF:
                    ve.tagged.append((tag, ev["tx"], ev["li"]))
            fps = memo_fingerprints(ev["p"])
            if fps:
                ve.memo_events += 1
                for fp in fps:
                    ve.memo_fps[fp] = ve.memo_fps.get(fp, 0) + 1
    return {k: v for k, v in out.items() if v.tagged or v.memo_events}


def collect_round_trips(events_by_ghost: Dict[str, List[dict]],
                        window_ledgers: int
                        ) -> Dict[Tuple[str, str], List[Tuple[str, str]]]:
    """§8 signal pass 2 — exact-amount relay matches across ALL ghosts:
    ghost A OUT precisely N to relay X, ghost B (A != B) IN precisely N from
    X, within +-window_ledgers (ledger-index truth, deterministic). BOTH
    legs' owners get relay-usage credit (cash-out and cash-in are both venue
    behavior). Dedupe on the (out_hash, in_hash) tx pair; pathological
    (amount, relay) buckets cap at _RT_BUCKET_CAP defensively."""
    ins: Dict[tuple, List[Tuple[str, int, str]]] = defaultdict(list)
    outs: List[Tuple[tuple, str, str, int, str]] = []
    for ghost, rows in events_by_ghost.items():
        for row in rows:
            ev = _event_view(row)
            if (ev is None or not ev["cp"] or ev["li"] <= 0
                    or ev["type"] != "Payment"
                    or ev["dir"] not in ("OUT", "IN")):
                continue
            amt = _amount_key(ev["p"])
            if amt is None:
                continue
            if ev["dir"] == "OUT":
                outs.append((amt, ev["cp"], ev["tx"], ev["li"], ghost))
            else:
                ins[(amt, ev["cp"])].append((ghost, ev["li"], ev["tx"]))
    outs.sort(key=lambda x: (x[3], x[2], x[1]))  # deterministic match order
    for bucket in ins.values():
        bucket.sort(key=lambda x: (x[1], x[2]))
    seen: Set[Tuple[str, str]] = set()
    res: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    w = max(1, int(window_ledgers))
    for amt, relay, tx_out, l_out, g_a in outs:
        for g_b, l_in, tx_in in ins.get((amt, relay), ())[:_RT_BUCKET_CAP]:
            if g_b == g_a or l_in == l_out or abs(l_out - l_in) > w:
                continue
            pair = (tx_out, tx_in)
            if pair in seen or (tx_in, tx_out) in seen:
                continue
            seen.add(pair)
            span = "L%d-L%d" % (min(l_out, l_in), max(l_out, l_in))
            res.setdefault((g_a, relay), []).append((g_b, span))
            res.setdefault((g_b, relay), []).append((g_a, span))
    return res


def fold_round_trips(ve_map: Dict[Tuple[str, str], VenueEvidence],
                     rt_map: Dict[Tuple[str, str], List[Tuple[str, str]]]):
    """Merge relay matches into the per-(ghost, venue) evidence map (a relay
    seen ONLY via round-trips materializes a VenueEvidence here)."""
    for key, hits in rt_map.items():
        ve = ve_map.get(key)
        if ve is None:
            ghost, venue = key
            ve = ve_map[key] = VenueEvidence(ghost, venue)
        ve.roundtrips.extend(hits)


def cross_ghost_index(ve_map) -> Tuple[dict, dict]:
    """Reuse registers that upgrade confidence: {(venue, tag): ghosts} and
    {(venue, memo_fp): ghosts} across ALL walked ghosts."""
    tag_gh: Dict[Tuple[str, int], Set[str]] = defaultdict(set)
    memo_gh: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for (ghost, venue), ve in ve_map.items():
        for tag, _tx, _li in ve.tagged:
            tag_gh[(venue, tag)].add(ghost)
        for fp in ve.memo_fps:
            memo_gh[(venue, fp)].add(ghost)
    return tag_gh, memo_gh


def evidence_score(ve: VenueEvidence, tag_shared: bool, memo_shared: bool,
                   cluster_n: int) -> float:
    """Deterministic §8 rubric (module header documents every weight)."""
    s = 0.30
    s += min(0.20, 0.05 * max(len(ve.tagged) - 1, 0))
    if tag_shared:
        s += 0.15
    if memo_shared:
        s += 0.10
    if ve.roundtrips:
        s += min(0.10, 0.05 * len(ve.roundtrips))
    if cluster_n >= 2:
        s += 0.05
    return min(_SCORE_MAX, round(s, 4))


def venue_label(ve: VenueEvidence, tag_gh: dict, memo_gh: dict) -> str:
    """Ledger-derived chip label, never a hardcoded venue name. Precedence:
    cross-ghost tag fingerprint > own deposit tags > shared-venue cluster tag
    > memo identity > pure relay; ties resolve deterministically (sorted
    iteration)."""
    own_tags = {t for t, _tx, _li in ve.tagged}
    cross_own, cross_own_gh, cross_any, cross_any_gh = None, 0, None, 0
    for (v, tag), ghosts in sorted(tag_gh.items(), key=lambda kv: (kv[0][1],)):
        if v != ve.venue or len(ghosts) < 2:
            continue
        if tag in own_tags:
            if cross_own is None or len(ghosts) > cross_own_gh:
                cross_own, cross_own_gh = tag, len(ghosts)
        elif cross_any is None or len(ghosts) > cross_any_gh:
            cross_any, cross_any_gh = tag, len(ghosts)
    if cross_own is not None:
        return "tag:%dx%d" % (cross_own, cross_own_gh)
    if own_tags:
        counts: Dict[int, int] = defaultdict(int)
        for t in ve.tagged:
            counts[t[0]] += 1
        top, cnt = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]
        return "tag:%dx%d" % (top, cnt)
    if cross_any is not None:
        return "tag:%dx%d" % (cross_any, cross_any_gh)
    cross_memo, best = None, 0
    for (v, fp), ghosts in sorted(memo_gh.items(), key=lambda kv: (kv[0][1],)):
        if v == ve.venue and len(ghosts) > best:
            cross_memo, best = fp, len(ghosts)
    if cross_memo:
        return "memo:%s" % cross_memo[:48]
    if ve.memo_fps:
        fp = sorted(ve.memo_fps.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        return "memo:%s" % fp[:48]
    return "relay"


def _census(ve: VenueEvidence, cluster_n: int, tag_partners: int,
            memo_partners: int) -> str:
    """Evidence census — the skip-compare key (idempotent re-runs, §4 parity).

    G4 (§15): emitted as canonical JSON ``{"v":1,"tags":{…}, …}`` so the notes
    payload carries a real structured object instead of baked TEXT. Per-tag
    histogram over ``ve.tagged`` (int keys) via a plain-dict pass — no new
    import; ``sort_keys=True`` sorts int keys numerically and emits them as
    JSON strings (e.g. ``{"v":1,"tags":{"1":128}}``). Tight separators +
    sort_keys make the string byte-stable for equality checks; control
    characters are JSON-escaped (notes stay _CTRL_RE-safe).
    """
    tags_hist: Dict[int, int] = {}
    for tag, _tx, _li in ve.tagged:
        tags_hist[tag] = tags_hist.get(tag, 0) + 1
    return json.dumps({"v": 1, "tags": tags_hist,
                       "tagged": len(ve.tagged),
                       "tag_distinct": len({t for t, _tx, _li in ve.tagged}),
                       "memos": ve.memo_events,
                       "fps": len(ve.memo_fps),
                       "roundtrips": len(ve.roundtrips),
                       "shared": cluster_n,
                       "tag_partners": tag_partners,
                       "memo_partners": memo_partners},
                      sort_keys=True, separators=(",", ":"))


def _notes_equal(stored, fresh: str) -> bool:
    """§15 — dict-level census equality, the skip-compare upgrade.

    v1 censuses are canonical (sort_keys, tight separators), so a plain string
    compare would also hold; dict-level is still the right contract — it
    forgives formatting drift and treats a legacy TEXT census (pre-v1) as
    unequal exactly once, driving exactly one remap to v1. Non-JSON garbage
    on the stored side falls back to raw string equality.
    """
    try:
        return json.loads(str(stored or "")) == json.loads(str(fresh or ""))
    except (ValueError, TypeError):
        return str(stored or "") == str(fresh or "")


def emit_constellation(tc: TopologyClient, emit_pairs: List[tuple],
                       dry_run: bool = False, force: bool = False,
                       log: Callable[[str], None] = _log) -> tuple:
    """Phase G3 — VENUE constellation emission (lodestone.md §13).

    One VENUE node + one VENUE_LINK edge per attributed (ghost, venue) pair,
    committed into the ghost's own constellation cluster. Account-scoped
    convention (module header): no token identity columns, no tx anchor.
    Idempotent like G2: unchanged census payloads are skipped; --force
    re-emits. Returns (nodes_written, edges_written, node_skips, edge_skips).
    """
    if not emit_pairs:
        log("[·] G3 constellation emission: 0 venue pairs — nothing to emit")
        return (0, 0, 0, 0)
    ghost_clusters = tc.ghost_cluster_ids()
    existing_nodes = {}
    for r in tc.latest_venue_nodes():
        existing_nodes[(str(r.get("cluster_id") or ""),
                        str(r.get("wallet_address") or ""))] = r
    existing_edges = {}
    for r in tc.latest_venue_edges():
        existing_edges[(str(r.get("cluster_id") or ""),
                        str(r.get("from_address") or ""),
                        str(r.get("to_address") or ""))] = r

    def _same(stored, fresh: str) -> bool:
        """Census-payload equality (formatting-tolerant dict compare)."""
        try:
            return json.loads(str(stored or "")) == json.loads(fresh)
        except ValueError:
            return False

    stamp = int(time.time())
    node_lines, edge_lines, node_skip, edge_skip = [], [], 0, 0
    for ghost, venue, label, score, census in emit_pairs:
        cluster = ghost_clusters.get(ghost) or ("C" + hashlib.sha256(
            ghost.encode("utf-8")).hexdigest()[:12])
        try:
            census_obj = json.loads(census)  # §15: nest v1 census as object
        except (ValueError, TypeError):
            census_obj = census  # legacy TEXT census rides as a string
        payload = json.dumps({"venue_label": label,
                              "evidence_score": round(float(score), 6),
                              "census": census_obj},
                             sort_keys=True, separators=(",", ":"))
        if (not force and (cluster, venue) in existing_nodes
                and _same(existing_nodes[(cluster, venue)]
                          .get("payload_json"), payload)):
            node_skip += 1
        else:
            node_lines.append(topology_node_line(
                stamp, cluster, venue, "VENUE", "", "", None, payload))
        if (not force and (cluster, ghost, venue) in existing_edges
                and _same(existing_edges[(cluster, ghost, venue)]
                          .get("payload_json"), payload)):
            edge_skip += 1
        else:
            edge_lines.append(topology_edge_line(
                stamp, cluster, "", "", "VENUE_LINK", ghost, venue,
                None, "", float(score), payload))
    if dry_run:
        log("[✓] dry-run: constellation would emit %d VENUE nodes + %d "
            "VENUE_LINK edges (%d node / %d edge unchanged skips)"
            % (len(node_lines), len(edge_lines), node_skip, edge_skip))
        return (len(node_lines), len(edge_lines), node_skip, edge_skip)
    before_n, before_e = (tc.count_rows("topology_nodes"),
                          tc.count_rows("topology_edges"))
    lines = node_lines + edge_lines
    if lines:
        tc.write(lines)
    n_nodes, n_edges = before_n, before_e
    for _ in range(4):  # WAL drain tolerance loop (graceful QuestDB handling)
        n_nodes, n_edges = (tc.count_rows("topology_nodes"),
                            tc.count_rows("topology_edges"))
        if (n_nodes >= before_n + len(node_lines)
                and n_edges >= before_e + len(edge_lines)):
            break
        time.sleep(2)
    log("[✓] verified: topology_nodes=%d (was %d, +%d VENUE) topology_edges="
        "%d (was %d, +%d VENUE_LINK) — census skips n=%d e=%d"
        % (n_nodes, before_n, len(node_lines), n_edges, before_e,
           len(edge_lines), node_skip, edge_skip))
    return (len(node_lines), len(edge_lines), node_skip, edge_skip)


def crossref_ghosts(cfg: WalkerConfig, ghost_filter: Optional[str] = None,
                    dry_run: bool = False, force: bool = False,
                    log: Callable[[str], None] = _log) -> List[tuple]:
    """Phase G2 pipeline: ghost_walks cache -> bounded ghost_events loads ->
    venue correlations (tag / memo / exact-amount relay) -> skip-unchanged ->
    ILP commit to ghost_venues -> WAL-drain verify. Returns report rows
    [(ghost, venue, label, score, tagged, memos, relays, xref, status)]."""
    tc = TopologyClient(QuestDbClient(
        cfg.qdb_exec_url, cfg.qdb_ilp_url, cfg.qdb_timeout_s,
        cfg.qdb_max_retries))
    eligible = {str(w.get("ghost_wallet") or ""):
                str(w.get("lineage_status") or "")
                for w in tc.latest_ghost_walks()
                if str(w.get("lineage_status") or "")
                in (STATUS_COMPLETE, STATUS_PARTIAL)}
    if ghost_filter and ghost_filter not in eligible:
        raise ValueError("ghost %r has no COMPLETE/PARTIAL ghost_walks row "
                         "(cache holds %d walked) — run: python3 -m walker "
                         "--ghost %s" % (ghost_filter, len(eligible),
                                         ghost_filter))
    ghosts = sorted([ghost_filter] if ghost_filter else eligible)
    if not ghosts:
        log("[x] no COMPLETE/PARTIAL ghost walks cached — "
            "run: python3 -m walker --ghost r…")
        return []
    log("[·] walked dossiers: %d (%s)"
        % (len(ghosts), ", ".join(g[:10] + "…" for g in ghosts)))
    events_by_ghost: Dict[str, List[dict]] = {}
    for g in ghosts:
        rows = tc.ghost_events(g, cfg.ghost_crossref_event_cap)
        events_by_ghost[g] = rows
        log("[·] %s…: %d evidence rows loaded" % (g[:10] + "…", len(rows)))
    ve_map = venue_evidence(events_by_ghost)
    fold_round_trips(ve_map, collect_round_trips(
        events_by_ghost, cfg.ghost_roundtrip_window_ledgers))
    tag_gh, memo_gh = cross_ghost_index(ve_map)
    log("[·] §8 signals: venue-pairs=%d tag-reuse=%d memo-reuse=%d"
        % (len(ve_map), sum(1 for _v, g in tag_gh.items() if len(g) > 1),
           sum(1 for _v, g in memo_gh.items() if len(g) > 1)))
    venue_ghosts: Dict[str, Set[str]] = defaultdict(set)
    for ghost, venue in ve_map:
        venue_ghosts[venue].add(ghost)
    existing = {(r.get("ghost_wallet"), r.get("venue_address")): r
                for r in tc.latest_venue_rows()}
    lines, report, emit_pairs, now = [], [], [], int(time.time())
    for key in sorted(ve_map):
        ghost, venue = key
        ve = ve_map[key]
        cluster_n = len(venue_ghosts[venue])
        partners = sorted(venue_ghosts[venue] - {ghost})[:_XREF_CAP]
        own_tags = {t for t, _tx, _li in ve.tagged}
        tp: Set[str] = set()  # ghosts sharing a tag value of ours here
        for (v, tag), gs in tag_gh.items():
            if v == venue and tag in own_tags:
                tp |= gs
        tp -= {ghost}
        mp: Set[str] = set()  # ghosts sharing a memo fingerprint of ours here
        for (v, fp), gs in memo_gh.items():
            if v == venue and fp in ve.memo_fps:
                mp |= gs
        mp -= {ghost}
        score = evidence_score(ve, bool(tp), bool(mp), cluster_n)
        label = venue_label(ve, tag_gh, memo_gh)
        notes = _census(ve, cluster_n, len(tp), len(mp))
        prev = existing.get(key)
        unchanged = False
        if prev:
            try:
                unchanged = (_notes_equal(prev.get("notes"), notes)
                             and str(prev.get("venue_label") or "") == label
                             and abs(float(prev.get("evidence_score") or 0.0)
                                     - score) < 1e-9)
            except (TypeError, ValueError):
                unchanged = False
        if unchanged and not force:
            status = "skip"
        else:
            status = "map"
            fps_str = ",".join(sorted(ve.memo_fps))[:1000]
            lines.append(ghost_venue_line(
                now, ghost, venue, label, score, len(ve.tagged), len(own_tags),
                ve.memo_events, fps_str, len(ve.roundtrips),
                ",".join("%s…" % a[:34] for a in partners), notes))
        report.append((ghost[:12] + "…", venue[:12] + "…", label,
                       "%.2f" % score, len(ve.tagged), ve.memo_events,
                       len(ve.roundtrips), "xref=%d" % len(partners), status))
        emit_pairs.append((ghost, venue, label, score, notes))
    if not ve_map:
        log("[x] no §8 venue signals in the cached dossiers (0 tagged "
            "deposits / memo identities / relay legs)")
        return report
    # Phase G3 — the constellation emission rides every crossref run (§13).
    emit_constellation(tc, emit_pairs, dry_run, force, log)
    if dry_run:
        log("[✓] dry-run: would write %d ghost_venues rows (%d unchanged "
            "skipped)" % (len(lines),
                          sum(1 for r in report if r[-1] == "skip")))
        for r in sorted(report, key=lambda x: -float(x[3]))[:12]:
            log("  [·] %s → %s [%s] conf=%s tagged=%s memos=%s relay=%s %s %s"
                % (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]))
        return report
    before = tc.count_rows("ghost_venues")
    if lines:
        tc.write(lines)
    after = before
    for _ in range(4):  # WAL drain tolerance loop (graceful QuestDB handling)
        after = tc.count_rows("ghost_venues")
        if after >= before + len(lines):
            break
        time.sleep(2)
    log("[✓] verified: ghost_venues=%d (was %d, wrote %d)"
        % (after, before, len(lines)))
    for r in sorted(report, key=lambda x: -float(x[3]))[:12]:
        log("  [✓] %s → %s [%s] conf=%s tagged=%s memos=%s relay=%s %s %s"
            % (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]))
    return report