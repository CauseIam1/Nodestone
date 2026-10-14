"""
Lodestone — structural transaction filter (blueprint §3 "Target Transaction Filters").

The issuer-account ledger stream is loud with swap and liquidity churn by design:
this module passes ONLY structural classes and stamps every sub-class inside
payload_json so tx_type stays blueprint-exact
('AMMCreate' | 'TrustSet' | 'Payment' | 'AccountSet'):

  AMMCreate  — day-zero pool creation: creator (actor), asset pair, created
               pseudo-account (AffectedNodes AMM node), fee setup.
  TrustSet   — holder trust-line creation referencing the issuer: early
               distribution network (walker keeps the earliest N).
  Payment    — genesis_funding : Payment that creates the issuer AccountRoot —
                                 the *ghost funding wallet* is its source (§4)
               token_issuance  : Payment FROM the issuer carrying the token
               token_return    : token Payment back TO the issuer (burn/backflow)
  AccountSet — issuer configuration adjustments, flags decoded for the dossier

Everything else (OfferCreate, AMMSwap/AMMBid/AMMVote/AMMDelete, NFToken*, etc.)
returns None and is counted only in walk statistics. Amount matching covers
Amount / DeliverMax / DeliverMin / SendMax envelopes. Stdlib only.
"""
from dataclasses import dataclass, field
from typing import Optional
from . import codenorm, meta

_ASF_FLAGS = {
    1: "asfRequireDest", 2: "asfRequireAuth", 3: "asfDisallowXRP",
    4: "asfDisableMaster", 7: "asfGlobalFreeze", 8: "asfNoFreeze",
    10: "asfDefaultRipple", 16: "asfAuthorizedNFTokenMinter", 17: "asfDisableClawback",
}
_LEGACY_TYPE_NAMES = {0: "Payment", 1: "OfferCreate", 3: "TrustSet", 4: "AccountSet"}


@dataclass
class StructuralHit:
    tx_type: str
    tx_hash: str
    actor_address: str
    ledger_index: int
    payload: dict = field(default_factory=dict)
    ripple_date: Optional[int] = None  # ledger `date`; resolved later when missing


def tx_type_of(tx: dict) -> str:
    t = tx.get("TransactionType")
    if t is None:
        t = tx.get("Type")
    if isinstance(t, int):  # pre-2013 integer enum; modern era unused, defensive
        return _LEGACY_TYPE_NAMES.get(t, f"Type_{int(t)}")
    return str(t) if t else "Unknown"


def amount_is_token(a) -> bool:
    return isinstance(a, dict) and bool(a.get("currency")) and bool(a.get("issuer"))


def amount_matches_token(a, currency: str, hex40: str, issuer: str) -> bool:
    if not amount_is_token(a):
        return False
    return (codenorm.currency_matches(a.get("currency"), currency, hex40)
            and str(a.get("issuer") or "") == issuer)


def _token_repr(a):
    if amount_is_token(a):
        return {"currency": a.get("currency"), "issuer": a.get("issuer"),
                "value": a.get("value")}
    if a is None:
        return None
    return {"drops": str(a)}  # XRP amounts arrive as integer drops


def _payment_amount_payload(tx: dict, md: dict) -> dict:
    """Amount envelope + delivered_amount truth for Payment payloads."""
    a = (tx.get("Amount") or tx.get("DeliverMax") or tx.get("DeliverMin")
         or tx.get("SendMax"))
    out = _token_repr(a) or {}
    dv = meta.delivered_amount(md)
    if dv is not None:
        out["delivered"] = _token_repr(dv)  # ledger truth beats envelope (house rule)
    return out

# ---------------------------------------------------------------------------
# Classification — one structural bucket per blueprint §3 filter class.


def classify_entry(entry: dict, *, currency: str, currency_hex40: str,
                   issuer: str) -> Optional[StructuralHit]:
    """Classify one account_tx entry into a StructuralHit, or None (structural only)."""
    tx = meta.entry_tx(entry)
    md = meta.entry_meta(entry)
    if not tx:
        return None
    ledger_index = meta.entry_ledger_index(entry)
    if not ledger_index:
        return None
    ttype = tx_type_of(tx)
    tx_hash = meta.entry_hash(entry)
    actor = str(tx.get("Account") or "")
    ripple_date = meta.entry_date(entry)

    if ttype == "AMMCreate":
        asset_a, asset_b = tx.get("Asset"), tx.get("Asset2")
        if not (amount_matches_token(asset_a, currency, currency_hex40, issuer)
                or amount_matches_token(asset_b, currency, currency_hex40, issuer)):
            return None
        amm = meta.created_amm_node(md) or {}
        payload = {"class": "amm_create",
                   "asset_a": _token_repr(asset_a), "asset_b": _token_repr(asset_b),
                   "amount_a": _token_repr(tx.get("Amount")),
                   "amount_b": _token_repr(tx.get("Amount2")),
                   "trading_fee": tx.get("TradingFee")}
        if amm.get("Account"):
            payload["amm_account"] = str(amm["Account"])
        return StructuralHit("AMMCreate", tx_hash, actor, ledger_index, payload,
                             ripple_date)

    if ttype == "TrustSet":
        la = tx.get("LimitAmount")
        if not amount_matches_token(la, currency, currency_hex40, issuer):
            return None
        holders = meta.created_trustline_holders(md, issuer)
        payload = {"class": "trustline",
                   "limit": {"currency": la.get("currency"), "issuer": la.get("issuer"),
                             "value": la.get("value")},
                   "new_trustline": actor in holders}
        return StructuralHit("TrustSet", tx_hash, actor, ledger_index, payload,
                             ripple_date)

    if ttype == "Payment":
        if issuer in meta.created_account_roots(md):
            # genesis: this Payment created the issuer AccountRoot — ghost anchor
            payload = {"class": "genesis_funding", **_payment_amount_payload(tx, md),
                       "destination": str(tx.get("Destination") or ""),
                       "created_account": issuer}
            return StructuralHit("Payment", tx_hash, actor, ledger_index, payload,
                                 ripple_date)
        if str(tx.get("Account")) == issuer and amount_matches_token(
                tx.get("Amount") or tx.get("DeliverMax"), currency, currency_hex40,
                issuer):
            payload = {"class": "token_issuance", **_payment_amount_payload(tx, md),
                       "destination": str(tx.get("Destination") or "")}
            return StructuralHit("Payment", tx_hash, actor, ledger_index, payload,
                                 ripple_date)
        if str(tx.get("Destination")) == issuer and amount_matches_token(
                tx.get("Amount") or tx.get("SendMax"), currency, currency_hex40,
                issuer):
            payload = {"class": "token_return", **_payment_amount_payload(tx, md),
                       "destination": issuer}
            return StructuralHit("Payment", tx_hash, actor, ledger_index, payload,
                                 ripple_date)
        return None

    if ttype == "AccountSet" and actor == issuer:
        set_f, clear_f = tx.get("SetFlag"), tx.get("ClearFlag")
        decoded = []
        if isinstance(set_f, int) and set_f in _ASF_FLAGS:
            decoded.append(_ASF_FLAGS[set_f])
        if isinstance(clear_f, int) and clear_f in _ASF_FLAGS:
            decoded.append("clear:" + _ASF_FLAGS[clear_f])
        payload = {"class": "issuer_config", "set_flag": set_f, "clear_flag": clear_f}
        if decoded:
            payload["flags_decoded"] = decoded
        return StructuralHit("AccountSet", tx_hash, actor, ledger_index, payload,
                             ripple_date)

    return None  # swap/liquidity noise — counted upstream, never recorded