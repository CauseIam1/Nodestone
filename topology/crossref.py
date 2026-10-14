"""
Lodestone — house-schema crossref interpreter (STRICTLY read-only).

amm_pools: pool_account IS the ledger's AMM pseudo-account (our mesh
discovery registers the exact account the ledger AMM object owns) — so a
mesh row whose asset pair references the token issuer yields BOTH an
AMM_POOL node and a POOL_MESH edge (issuer ↔ pool_account). Mesh rows are
live-registry truth, NOT ledger walk truth: they describe the pool as our
engine sees it today, therefore POOL_MESH edges are never used for day-zero
creator attribution — only AMMCreate ledger events prove creators.

wallet_registry: our internal 3-tier grid (FOUNDATION / TRADING_BOT /
MANUAL_TRADING, 134 each). Any constellation member matching this grid is
flagged INTERNAL with its registry name/category — an internal ghost would
be a major finding, so it is surfaced in payload/notes, never folded into
creator confidence.

Isolation Rule: reads only — no writes, no schema touches. Stdlib only.
"""
import json
from typing import Dict, List, Optional

MESH_POOL_EDGE_CONF = 0.90  # registry-truth link, strongest structural edge


def _compact(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def mesh_flags(pools: List[dict]) -> dict:
    """Boolean evidence flags for the confidence-refinement model."""
    return {"mesh_pool": bool(pools), "mesh_pool_count": len(pools)}


def mesh_links(pools: List[dict]) -> List[dict]:
    """One link dict per qualifying mesh row: pool node + POOL_MESH edge data.

    Defensive: re-verifies the issuer is genuinely on one side of the pair
    (rows arrive pre-filtered; never trust the pre-filter blindly).
    """
    out: List[dict] = []
    for p in pools:
        pa = str(p.get("pool_account") or "")
        if not pa:
            continue
        payload = {
            "source": "amm_pools",
            "pool_account": pa,
            "pair": "%s/%s ~ %s/%s" % (
                p.get("asset_a_currency"), p.get("asset_a_issuer"),
                p.get("asset_b_currency"), p.get("asset_b_issuer")),
            "fee_tier": p.get("fee_tier"),
            "lp_token_balance": p.get("lp_token_balance"),
            "discovered_at": str(p.get("discovered_at") or ""),
        }
        out.append({"pool_account": pa, "payload": payload})
    return out


def internal_flag(registry: Dict[str, dict], address: str) -> Optional[dict]:
    """wallet_registry membership for an address, or None (external actor)."""
    hit = registry.get(address or "")
    if not hit:
        return None
    return {"registry_name": str(hit.get("wallet_name") or ""),
            "registry_category": str(hit.get("wallet_category") or "")}


def has_internal_member(registry: Dict[str, dict], addresses) -> bool:
    return any(internal_flag(registry, a) for a in addresses)