"""
Lodestone Phase 3 — Multi-Account Topology Mapping (lodestone.md §4).

Correlates the three §4 account layers per walked token — ISSUER, the
genesis-funding GHOST wallet and the AMM layer (day-zero AMM_CREATOR +
AMM_POOL pseudo-account) — plus the earliest HOLDER distribution front,
then joins tokens sharing the SAME ghost wallet into one cluster_id
(repeat-creator detection) and refines the walker's creator confidence
with cross-account topology signals.

Phase G2 (lodestone.md §8): ghost_crossref.py crossrefs ALL walked ghost
dossiers into ghost_venues — ledger-evidence venue attribution (tag / memo
fingerprints + exact-amount round-trips; never hardcoded venue address
lists) plus same-exit clusters via cross-ghost reuse.

Isolation Rule (Phase 3 + G2 widening — see topology/qdbx.py):
  reads:  token_lineage + forensic_audit_events + ghost_walks + ghost_events
          (Lodestone-owned) and amm_pools + wallet_registry (house tables,
          STRICTLY read-only)
  writes: topology_nodes + topology_edges + token_lineage + ghost_venues
          (Lodestone-owned)
Run from the Lodestone/ directory:  python3 -m topology --help
Stdlib only.
"""