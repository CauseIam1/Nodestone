"""
Lodestone — ledger metadata (AffectedNodes) parser.

Blueprint §3 Metadata Parsing: "Inspects transaction metadata (AffectedNodes),
specifically parsing CreatedNode entries for new ledger objects and
PreviousFields for reserve adjustments." Extracts exactly the object types the
§4 topology mapping needs:
    AccountRoot  -> issuer existence + creation-node detection (ghost anchor)
    RippleState  -> trust lines (holder network, both line directions)
    AMM          -> day-zero pool pseudo-account + asset pair
and surfaces meta.delivered_amount — ledger-truth delivered value (house rule:
register delivered value from ledger truth, never the Amount envelope).
Stdlib only.
"""
import json
from typing import Optional, Set


def entry_tx(entry: dict) -> dict:
    """Raw transaction container across JSON-RPC generations (tx / tx_json)."""
    return entry.get("tx") or entry.get("tx_json") or {}


def entry_meta(entry: dict) -> dict:
    """Metadata container across generations (meta / metaData)."""
    return entry.get("meta") or entry.get("metaData") or {}


def entry_hash(entry: dict) -> str:
    h = entry.get("hash") or entry_tx(entry).get("hash") or ""
    return str(h)


def entry_ledger_index(entry: dict):
    """Ledger sequence of the tx — entry level (v1) or tx level (tx-only archive)."""
    li = entry.get("ledger_index")
    if li is None:
        li = entry_tx(entry).get("ledger_index")
    if isinstance(li, (int, float)):
        return int(li)
    try:
        return int(str(li))
    except (TypeError, ValueError):
        return None


def entry_date(entry: dict):
    """Close time (ripple epoch) from entry (v1) or the tx object itself."""
    d = entry.get("date")
    if d is None:
        d = entry_tx(entry).get("date")
    return int(d) if isinstance(d, (int, float)) else None


def affected_nodes(meta_md: dict) -> list:
    return (meta_md or {}).get("AffectedNodes") or []


def _op_fields(node: dict):
    for key in ("CreatedNode", "ModifiedNode", "DeletedNode"):
        if key in node:
            return key, node[key]
    return None, {}


def created_account_roots(meta_md: dict) -> Set[str]:
    """Addresses of AccountRoot objects CREATED by this transaction."""
    out: Set[str] = set()
    for n in affected_nodes(meta_md):
        op, fields = _op_fields(n)
        if op == "CreatedNode" and fields.get("LedgerEntryType") == "AccountRoot":
            a = (fields.get("NewFields") or {}).get("Account")
            if a:
                out.add(str(a))
    return out


def created_amm_node(meta_md: dict) -> Optional[dict]:
    """NewFields of a CreatedNode AMM object (pool account + asset pair), or None."""
    for n in affected_nodes(meta_md):
        op, fields = _op_fields(n)
        if op == "CreatedNode" and fields.get("LedgerEntryType") == "AMM":
            return dict(fields.get("NewFields") or {})
    return None


def created_trustline_holders(meta_md: dict, issuer: str) -> Set[str]:
    """Holder addresses of CREATED RippleState nodes involving `issuer` on either
    side — the distribution-network edges TrustSet/issuance payments build."""
    out: Set[str] = set()
    for n in affected_nodes(meta_md):
        op, fields = _op_fields(n)
        if op != "CreatedNode" or fields.get("LedgerEntryType") != "RippleState":
            continue
        nf = fields.get("NewFields") or {}
        low = nf.get("LowLimit") or {}
        high = nf.get("HighLimit") or {}
        if str(low.get("issuer") or "") == issuer:
            holder = high.get("Account")
        elif str(high.get("issuer") or "") == issuer:
            holder = low.get("Account")
        else:
            continue
        if holder:
            out.add(str(holder))
    return out


def delivered_amount(meta_md: dict):
    """meta.delivered_amount when present (partial-payment truth); else None."""
    return (meta_md or {}).get("delivered_amount")


def node_summary(meta_md: dict) -> dict:
    """Compact op/type census for payload_json (bounded, order-stable)."""
    out = {"created": {}, "modified": {}, "deleted": {}}
    for n in affected_nodes(meta_md):
        op, fields = _op_fields(n)
        if not op:
            continue
        et = str(fields.get("LedgerEntryType") or "?")
        bucket = out["created" if op == "CreatedNode" else
                     ("modified" if op == "ModifiedNode" else "deleted")]
        bucket[et] = bucket.get(et, 0) + 1
    return {k: v for k, v in out.items() if v}


def compact_json(obj) -> str:
    """Payload canonicalizer — sorted keys, no whitespace, UTF-8 safe."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)