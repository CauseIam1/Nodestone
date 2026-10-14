"""
Lodestone Topology Mapper — Phase 3 orchestration (lodestone.md §4).

Pipeline: token_lineage LATEST × forensic_audit_events → constellation
nodes/edges → house crossrefs (amm_pools, wallet_registry; READ-ONLY) →
ghost-wallet cluster join → ILP commit (topology_nodes / topology_edges)
+ refined token_lineage version rows (LATEST ON always wins; unchanged
notes are skipped, so re-runs are idempotent; --force re-maps clusters).

Refined creator-confidence (deterministic; on top of the walk score — walk
evidence stays dominant, refinement only ADDS topology signals):
    walk_conf
  + 0.10 mesh pool_account linked to the issuer (amm_pools crossref)
  + 0.05 day-zero AMM created BY the ghost (AMMCreate actor == ghost)
  + 0.05 × extra tokens sharing the same ghost cluster (cap 0.15)
  + 0.05 ≥ 2 genesis funding Payments (multi-funding signature)
  + 0.05 ghost == first token-issuance destination (re-engagement)
  clamp ≤ 0.98
Writes (Isolation Rule): topology_nodes / topology_edges / token_lineage
ONLY. amm_pools + wallet_registry are read-only house looks. Stdlib only.
"""
import time
from collections import defaultdict

from walker.qdb import QuestDbClient, lineage_line
from walker.config import WalkerConfig
from .qdbx import TopologyClient, topology_node_line, topology_edge_line
from .graph import build_token_graph
from .crossref import has_internal_member


def _log(line):
    print("[lodestone-topology %s] %s"
          % (time.strftime('%H:%M:%S', time.gmtime()), line))


def refine_confidence(walk_conf, ev, extra_cluster_tokens):
    """Documented Phase 3 refinement rubric (see module header). Never lowers
    walk evidence and never exceeds 0.98 — absolute ledger proof is reserved
    for Phase 3+ multi-source confirmations."""
    boost = 0.0
    if ev.get("mesh_pool"):
        boost += 0.10
    if ev.get("amm_creator_ghost"):
        boost += 0.05
    boost += min(0.05 * max(int(extra_cluster_tokens or 0), 0), 0.15)
    if ev.get("genesis_funding_count", 0) >= 2:
        boost += 0.05
    if ev.get("ghost_first_dest"):
        boost += 0.05
    return min(0.98, round(float(walk_conf) + boost, 4))


def _opt_int(v):
    try:
        return int(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _log_cluster(log, cid, gs, state):
    """Black-Box style constellation feed for one cluster."""
    ghost = (str(gs[0]["lineage_row"].get("funding_wallet") or "")
             if gs else "")
    tag = " REPEAT-CREATOR" if len(gs) > 1 else ""
    log("=== cluster %s%s — ghost=%s shared_tokens=%d %s"
        % (cid, tag, ghost or "solo", len(gs), state if state else "mapping"))
    flat = []
    for g in sorted(gs, key=lambda x: (str(x["issuer"]),
                                       str(x["currency"]))):
        for e in sorted(g["edges"], key=lambda x: (x["ledger_index"] or 0,
                                                   x["edge_type"])):
            flat.append((g["currency"], e))
    for _unused, e in flat[:60]:
        log("  [✓] %-15s %s → %s @L%s via %s"
            % (e["edge_type"], e["from_address"], e["to_address"],
               e["ledger_index"] or "-", e["tx_hash"] or "mesh-registry"))
    if len(flat) > 60:
        log("  [·] +%d more edges held in topology_edges" % (len(flat) - 60))


def _assemble(clusters, existing, now, registry, dry_run, force, log):
    """Per-cluster commit build: ILP lines + refined-lineage rows + report."""
    node_lines, edge_lines, refined_lines, report = [], [], [], []
    for cid in sorted(clusters):
        gs = sorted(clusters[cid],
                    key=lambda g: (str(g["issuer"]), str(g["currency"])))
        already = cid in existing
        write = force or not already
        internal = any(has_internal_member(
            registry, [n["wallet_address"] for n in g["nodes"]]) for g in gs)
        _log_cluster(log, cid, gs, "" if write else "ALREADY MAPPED — refine only")
        for g in gs:
            lin = g["lineage_row"]
            walk_conf = float(lin.get("creator_confidence") or 0.10)
            status = str(lin.get("lineage_status") or "")
            refined = refine_confidence(walk_conf, g["evidence"], len(gs) - 1)
            mesh_n = int(g["evidence"].get("mesh_pool_count") or 0)
            notes = ("topology: cluster=%s shared_ghost_tokens=%d "
                     "mesh_pool=%d amm_by_ghost=%s multi_genesis_funding=%s"
                     " ghost_first_dest=%s internal_overlap=%s"
                     % (cid, len(gs) - 1, mesh_n,
                        int(bool(g["evidence"].get("amm_creator_ghost"))),
                        int(g["evidence"].get("genesis_funding_count", 0) >= 2),
                        int(bool(g["evidence"].get("ghost_first_dest"))),
                        int(internal)))
            if write:
                for n in g["nodes"]:
                    node_lines.append(topology_node_line(
                        now, cid, n["wallet_address"], n["role"],
                        g["currency"], g["issuer"], n.get("ledger_index"),
                        n["payload_json"]))
                for e in g["edges"]:
                    edge_lines.append(topology_edge_line(
                        now, cid, g["currency"], g["issuer"], e["edge_type"],
                        e["from_address"], e["to_address"],
                        e.get("ledger_index"), e.get("tx_hash") or "",
                        e["edge_confidence"], e["payload_json"]))
            # Refined lineage: unchanged notes => idempotent skip
            if not dry_run and str(lin.get("notes") or "") != notes:
                refined_lines.append(lineage_line(
                    now, g["currency"], g["issuer"],
                    g["ghost"] or str(lin.get("funding_wallet") or ""),
                    str(lin.get("amm_account") or ""),
                    _opt_int(lin.get("amm_create_ledger")), refined, status,
                    notes))
            report.append((g["currency"], cid, g["ghost"] or "-",
                           "shared=%d" % (len(gs) - 1),
                           "skip" if already else "map",
                           "%d→%d" % (walk_conf * 100, refined * 100),
                           "mesh=%d" % mesh_n,
                           "int" if internal else "ext", status))
    return node_lines, edge_lines, refined_lines, report


def map_all(cfg, token_filter=None, dry_run=False, force=False, log=_log):
    """Phase 3 pipeline: load → constellation graph → cluster-join → crossref
    → commit (nodes/edges + refined lineage rows). Returns report rows."""
    tc = TopologyClient(QuestDbClient(cfg.qdb_exec_url, cfg.qdb_ilp_url,
                                      cfg.qdb_timeout_s, cfg.qdb_max_retries))
    rows = tc.latest_lineage()
    if token_filter:
        k = str(token_filter).casefold()
        rows = [r for r in rows
                if str(r.get("currency") or "").casefold() == k]
        if not rows:
            raise ValueError("no walked token matches --token %r (see --list)"
                             % token_filter)
    if not rows:
        log("[x] token_lineage empty — run the Phase 2 walker first")
        return []
    log("[·] walked tokens in cache: %d" % len(rows))
    registry = tc.registry_map()
    log("[·] house crossrefs loaded: wallet_registry=%d addresses (read-only)"
        % len(registry))
    graphs = []
    for r in rows:
        events = tc.audit_events(r.get("currency") or "",
                                 r.get("issuer") or "")
        pools = tc.mesh_pools(r.get("issuer") or "")
        g = build_token_graph(r, events, pools, registry)
        graphs.append(g)
        log("[·] %s: nodes=%d edges=%d evidence=%s"
            % (g["currency"], len(g["nodes"]), len(g["edges"]), g["evidence"]))
    clusters = defaultdict(list)
    for g in graphs:
        clusters[g["cluster_id"]].append(g)
    existing = set(tc.cluster_ids())
    node_lines, edge_lines, refined_lines, report = _assemble(
        clusters, existing, int(time.time()), registry, dry_run, force, log)
    if dry_run:
        log("[✓] dry-run: would write %d nodes, %d edges, %d refined lineage"
            " rows" % (len(node_lines), len(edge_lines), len(refined_lines)))
        return report
    before_n, before_e = (tc.count_rows("topology_nodes"),
                          tc.count_rows("topology_edges"))
    tc.write(node_lines)
    tc.write(edge_lines)
    tc.write(refined_lines)
    log("[✓] committed %d nodes / %d edges / %d refined lineage rows"
        % (len(node_lines), len(edge_lines), len(refined_lines)))
    n_now, e_now = before_n, before_e
    for _ in range(4):  # WAL drain tolerance (graceful QuestDB handling)
        n_now, e_now = (tc.count_rows("topology_nodes"),
                        tc.count_rows("topology_edges"))
        if (n_now >= before_n + len(node_lines)
                and e_now >= before_e + len(edge_lines)):
            break
        time.sleep(2)
    log("[✓] verified: topology_nodes=%d (was %d) topology_edges=%d (was %d)"
        % (n_now, before_n, e_now, before_e))
    if refined_lines and graphs:
        r0 = tc.latest_lineage_row(graphs[0]["currency"], graphs[0]["issuer"])
        if r0:
            log("[✓] verified LATEST lineage %s/%s conf=%.2f notes=%s"
                % (r0.get("currency"), r0.get("issuer"),
                   r0.get("creator_confidence") or 0.0,
                   str(r0.get("notes") or "")[:90]))
    return report