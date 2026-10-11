"""
Lodestone Topology — QuestDB bridge (Phase 3 widened read contract).

The Phase 1/2 walk transport (walker/qdb.py) is untouched and its contract
stays narrow. The Phase 3 mapper (lodestone.md §4) adds two STRICTLY
read-only house crossrefs:
    amm_pools       -> mesh pool_account ↔ issuer/asset-pair linkage
    wallet_registry -> internal-overlap check against our 3-tier grid
Both are pure SELECTs over GET /exec (:9000) — no writes, no contention with
the live engine. Phase 3 writes remain strictly Lodestone-owned:
    topology_nodes, topology_edges + refined token_lineage rows
(ILP over /write?precision=ns — same merge-dedupe transport as the walker).
Stdlib only.
"""
from typing import List, Optional

from walker.qdb import (QuestDbClient, QdbError, ilp_float, ilp_long,
                        ilp_string, lineage_line, sql_str)


def _esc_tag(v) -> str:
    """ILP symbol/tag escape (QuestDB ILP grammar): backslash, space, comma."""
    s = "" if v is None else str(v)
    return s.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")


def topology_node_line(ts_unix: int, cluster_id: str, wallet_address: str,
                       role: str, currency: str, issuer: str,
                       ledger_index: Optional[int], payload_json: str) -> str:
    """One topology_nodes ILP line (mapped_at = mapper commit time)."""
    tags = ("topology_nodes,cluster_id=%s,wallet_address=%s,role=%s,"
            "currency=%s,issuer=%s" % (_esc_tag(cluster_id),
                                       _esc_tag(wallet_address), _esc_tag(role),
                                       _esc_tag(currency), _esc_tag(issuer)))
    fields = []
    if ledger_index is not None:
        fields.append("ledger_index=%s" % ilp_long(ledger_index))
    fields.append("payload_json=%s" % ilp_string(payload_json))
    return "%s %s %d" % (tags, ",".join(fields), int(ts_unix) * 1_000_000_000)


def topology_edge_line(ts_unix: int, cluster_id: str, currency: str,
                       issuer: str, edge_type: str, from_address: str,
                       to_address: str, ledger_index: Optional[int],
                       tx_hash: str, edge_confidence: float,
                       payload_json: str) -> str:
    """One topology_edges ILP line (tx_hash empty for mesh/registry links)."""
    tags = ("topology_edges,cluster_id=%s,currency=%s,issuer=%s,edge_type=%s,"
            "from_address=%s,to_address=%s,tx_hash=%s"
            % (_esc_tag(cluster_id), _esc_tag(currency), _esc_tag(issuer),
               _esc_tag(edge_type), _esc_tag(from_address),
               _esc_tag(to_address), _esc_tag(tx_hash)))
    fields = []
    if ledger_index is not None:
        fields.append("ledger_index=%s" % ilp_long(ledger_index))
    fields.append("edge_confidence=%s" % ilp_float(edge_confidence))
    fields.append("payload_json=%s" % ilp_string(payload_json))
    return "%s %s %d" % (tags, ",".join(fields), int(ts_unix) * 1_000_000_000)


def _rows(exec_result: dict) -> List[dict]:
    """Zip /exec columns+dataset into row dicts (defensive empty handling)."""
    cols = [c.get("name") for c in (exec_result.get("columns") or [])]
    out: List[dict] = []
    for vals in exec_result.get("dataset") or []:
        d = {c: v for c, v in zip(cols, vals)}
        if d:
            out.append(d)
    return out


class TopologyClient:
    """QuestDB read/write surface for Phase 3 mapping runs."""

    def __init__(self, qdb_client):
        self.qdb = qdb_client

    # ---- Lodestone-owned reads -------------------------------------------
    def latest_lineage(self) -> List[dict]:
        """All walked tokens collapsed to their LATEST lineage version."""
        return _rows(self.qdb.exec_sql(
            "select * from token_lineage "
            "LATEST ON discovered_at PARTITION BY issuer, currency"))

    def audit_events(self, currency: str, issuer: str,
                     limit: int = 10000) -> List[dict]:
        """Chronological structural events for one token (walk evidence)."""
        sql = ("select * from forensic_audit_events where issuer='%s' and "
               "currency='%s' order by timestamp asc limit %d"
               % (sql_str(issuer), sql_str(currency), max(1, int(limit))))
        return _rows(self.qdb.exec_sql(sql))

    def latest_lineage_row(self, currency: str, issuer: str) -> Optional[dict]:
        sql = ("select * from token_lineage where issuer='%s' and "
               "currency='%s' LATEST ON discovered_at "
               "PARTITION BY issuer, currency"
               % (sql_str(issuer), sql_str(currency)))
        rows = _rows(self.qdb.exec_sql(sql))
        return rows[0] if rows else None

    # ---- Phase 3 table probes (cluster dedupe + write verification) ------
    def cluster_ids(self) -> List[str]:
        ds = (self.qdb.exec_sql(
            "select distinct cluster_id from topology_nodes").get("dataset")
            or [])
        return [r[0] for r in ds if r and r[0]]

    def count_rows(self, table: str) -> int:
        n = self.qdb.scalar("select count() from %s" % table, default=0)
        try:
            return int(n or 0)
        except (TypeError, ValueError):
            return 0

    # ---- House reads: CROSSREF ONLY (strictly SELECT, never written) -----
    def mesh_pools(self, issuer: str) -> List[dict]:
        sql = ("select pool_account, asset_a_currency, asset_a_issuer, "
               "asset_b_currency, asset_b_issuer, lp_token_balance, fee_tier, "
               "discovered_at from amm_pools where asset_a_issuer='%s' or "
               "asset_b_issuer='%s'" % (sql_str(issuer), sql_str(issuer)))
        return _rows(self.qdb.exec_sql(sql))

    def registry_map(self) -> dict:
        """402-row house wallet grid keyed by address (read-only snapshot)."""
        rows = _rows(self.qdb.exec_sql(
            "select wallet_address, wallet_name, wallet_category "
            "from wallet_registry"))
        return {r["wallet_address"]: r for r in rows
                if r.get("wallet_address")}

    # ---- ILP writes (Lodestone-owned Phase 3 tables only) -----------------
    def write(self, lines: List[str]):
        self.qdb.write_lines(lines)