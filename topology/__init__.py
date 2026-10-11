"""
Lodestone Phase 3 — Multi-Account Topology Mapping (lodestone.md §4).

Correlates the three §4 account layers per walked token — ISSUER, the
genesis-funding GHOST wallet and the AMM layer (day-zero AMM_CREATOR +
AMM_POOL pseudo-account) — plus the earliest HOLDER distribution front,
then joins tokens sharing the SAME ghost wallet into one cluster_id
(repeat-creator detection) and refines the walker's creator confidence
with cross-account topology signals.

Isolation Rule (Phase 3 widening, reads only — see topology/qdbx.py):
  reads:  token_lineage + forensic_audit_events (Lodestone-owned) and
          amm_pools + wallet_registry (house tables, STRICTLY read-only)
  writes: topology_nodes + topology_edges + token_lineage (Lodestone-owned)
Run from the Lodestone/ directory:  python3 -m topology --help
Stdlib only.
"""