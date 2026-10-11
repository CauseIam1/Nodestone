"""
Lodestone — constellation graph builder (pure functions, no I/O). §4.

Correlated account layers per token:
    ISSUER      minting account (custom currency definitions)
    GHOST       external origin wallet that funded the issuer AccountRoot
    HOLDER      earliest distribution front (TrustSet + first issuances)
    AMM_CREATOR day-zero pool creator (AMMCreate actor — ledger truth)
    AMM_POOL    pool pseudo-account (created AMM node, or mesh crossref)

Cluster identity: "C" + sha256(ghost_wallet)[:12]. The SAME ghost reused
across issuers joins those tokens into ONE cluster_id — §4's "cross-
referencing ghost wallets across multiple tokens exposes repeat creators".
Ghost-less tokens keep an isolated per-token cluster ("solo:…" basis).

Edge-confidence rubric (evidence classes, deterministic):
    GENESIS_FUNDING 0.60  ISSUANCE 0.30  TRUST 0.20  AMM_CREATE 0.80
    POOL_MESH 0.90 (mesh-registry link — never a creator proof)
Stdlib only.
"""
import hashlib
import json

from .crossref import internal_flag, mesh_links

EDGE_CONF = {
    "GENESIS_FUNDING": 0.60,   # parity with the walker's genesis attribution
    "ISSUANCE": 0.30,
    "TRUST": 0.20,
    "AMM_CREATE": 0.80,
    "POOL_MESH": 0.90,
}


def cluster_id_for(ghost, currency, issuer) -> str:
    src = (ghost or "").strip()
    basis = src if src else "solo:%s|%s" % (issuer, currency)
    return "C" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:12]


def _jdump(obj) -> str:
    """Payload canonicalizer — sorted keys, no whitespace (payload parity)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def _parse_payload(raw):
    try:
        return json.loads(raw or "{}")
    except ValueError:
        return {"class": "unparsed", "raw": str(raw or "")[:200]}


def _amount_repr(p: dict):
    """Amount envelope or ledger-truth delivered value, whichever exists."""
    return (p.get("delivered") if p.get("delivered") is not None
            else (p.get("drops") if p.get("drops") is not None
                  else p.get("amount")))


def build_token_graph(lineage_row: dict, events: list, pools: list,
                      registry: dict) -> dict:
    """Deterministic constellation for ONE token from pre-fetched evidence."""
    currency = str(lineage_row.get("currency") or "")
    issuer = str(lineage_row.get("issuer") or "")
    ghost = str(lineage_row.get("funding_wallet") or "")
    nodes, edges, seen_nodes = [], [], set()

    def add_node(wallet, role, ledger, payload):
        if not wallet:
            return
        if (wallet, role) in seen_nodes:  # earliest evidence wins, dedupe
            return
        seen_nodes.add((wallet, role))
        nodes.append({"wallet_address": wallet, "role": role,
                      "ledger_index": ledger,
                      "payload_json": _jdump(payload or {})})

    def add_edge(frm, to, etype, ledger, tx_hash, payload):
        edges.append({"edge_type": etype, "from_address": frm,
                      "to_address": to, "ledger_index": ledger,
                      "tx_hash": tx_hash or "",
                      "edge_confidence": EDGE_CONF[etype],
                      "payload_json": _jdump(payload or {})})

    add_node(issuer, "ISSUER", None, {"kind": "issuer_account"})
    gens, first_dest, amm_by_ghost, ghost_ev, holder_n = 0, None, False, "", 0
    for ev in sorted(events, key=lambda e: (e.get("ledger_index") or 0,
                                            str(e.get("tx_type") or ""),
                                            str(e.get("actor_address") or ""))):
        p = _parse_payload(str(ev.get("payload_json") or ""))
        cls = str(p.get("class") or "")
        actor = str(ev.get("actor_address") or "")
        li = ev.get("ledger_index")
        li = int(li) if li not in (None, "") else None
        tx = str(ev.get("tx_hash") or "")
        gh = ghost or ghost_ev
        if cls == "genesis_funding":
            gens += 1
            ghost_ev = actor
            add_node(actor, "GHOST", li, {"kind": "genesis_funder",
                                          "amount": _amount_repr(p)})
            add_node(issuer, "ISSUER", li, None)
            add_edge(actor, str(p.get("created_account") or issuer),
                     "GENESIS_FUNDING", li, tx,
                     {"delivered": _amount_repr(p),
                      "destination": p.get("destination")})
        elif cls == "token_issuance":
            dest = str(p.get("destination") or "")
            if dest:
                first_dest = first_dest or dest  # earliest issuance (sorted)
                holder_n += 1
                add_node(dest, "HOLDER", li, {"kind": "issuance_receiver"})
                add_edge(issuer, dest, "ISSUANCE", li, tx,
                         {"delivered": _amount_repr(p)})
        elif cls == "token_return":
            continue  # backflow noise — counted upstream, not constellation
        elif cls == "trustline":
            holder_n += 1
            add_node(actor, "HOLDER", li, {"kind": "trustline_holder"})
            add_edge(actor, issuer, "TRUST", li, tx, {"limit": p.get("limit")})
        elif cls == "amm_create":
            pool = str(p.get("amm_account") or "")
            amm_by_ghost = amm_by_ghost or (gh and actor == gh)
            add_node(actor, "AMM_CREATOR", li, {"kind": "dayzero_creator"})
            if pool:
                add_node(pool, "AMM_POOL", li, {"source": "ledger_AMMCreate"})
                add_edge(actor, pool, "AMM_CREATE", li, tx,
                         {"trading_fee": p.get("trading_fee")})
    # Mesh crossref: AMM_POOL nodes + POOL_MESH edges (registry truth — the
    # live pool the engine sees today, NOT a day-zero creator proof).
    for link in mesh_links(pools):
        add_node(link["pool_account"], "AMM_POOL", None, dict(link["payload"]))
        add_edge(issuer, link["pool_account"], "POOL_MESH", None, "",
                 link["payload"])
    # Internal-overlap stamps (house grid) — flagged, never confidence inputs.
    for n in nodes:
        hit = internal_flag(registry, n["wallet_address"])
        if hit:
            try:
                base = json.loads(n["payload_json"])
            except ValueError:
                base = {}
            base["internal"] = hit
            n["payload_json"] = _jdump(base)
    evidence = {
        "mesh_pool": bool(pools), "mesh_pool_count": len(pools),
        "amm_creator_ghost": amm_by_ghost,
        "genesis_funding_count": gens,
        "ghost_first_dest": bool(first_dest and (ghost or ghost_ev)
                                 and first_dest == (ghost or ghost_ev)),
        "holders": holder_n,
    }
    return {"currency": currency, "issuer": issuer,
            "ghost": ghost or ghost_ev,
            "cluster_id": cluster_id_for(ghost or ghost_ev, currency, issuer),
            "nodes": nodes, "edges": edges,
            "lineage_row": lineage_row, "evidence": evidence}