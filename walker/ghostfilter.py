"""
Lodestone — ghost-mode structural filter (Phase G1, lodestone.md §7).

filter.py looks through an ISSUER lens (token-matched structural classes only).
This module looks through a GHOST-account lens — account-centric, token-blind,
because ghost dossiers need every Payment the wallet ever sent or received in
XRP or any IOU, plus every TrustSet it signed:

  Payment  — passes always; stamped with ghost-relative DIRECTION:
               OUT  : ghost submitted the Payment      (counterparty = dest)
               IN   : the ghost received the Payment   (counterparty = source)
               SELF : both sides are the ghost
               OTHER: ledger pathway involvement on neither side (defensive —
                      kept so no evidence is silently dropped)
             DestinationTag / SourceTag (per-user deposit fingerprints; Phase G2
             venue-correlation material) are lifted into payload_json, and
             Memos[] are hex-decoded (XRPL memo fields are hex wire form —
             venues like Binance put the deposit ID in MemoData).
  TrustSet — passes always: holder-side lines (ghost as issuer) and issuer-side
             lines (ghost building a line) both fingerprint wallet behavior.

Everything else (OfferCreate, AMM*, NFToken*, etc.) returns None and is counted
only in walk statistics. `delivered_amount` ledger truth beats the Amount
envelope (house rule). tx_type stays blueprint-exact ('Payment' | 'TrustSet');
sub-classes ride inside payload_json ('ghost_payment' | 'ghost_trustline').
Stdlib only.
"""
import binascii
from typing import List, Optional

from . import meta
from .filter import StructuralHit, amount_is_token, tx_type_of

_MEMO_FIELD_CAP = 512   # decoded characters kept per memo field
_MEMO_COUNT_CAP = 16    # memos kept per Payment
_TAG_MAX = 2 ** 32 - 1  # DestinationTag / SourceTag are uint32
_HEX_CHARS = "0123456789abcdefABCDEF"


def _decode_memo_field(v) -> Optional[dict]:
    """One memo field: XRPL hex wire form -> {"text": ...}, else raw {"hex": ...}."""
    if not isinstance(v, str) or v == "":
        return None
    if not (len(v) % 2 == 0 and all(ch in _HEX_CHARS for ch in v)):
        return {"text": v[:_MEMO_FIELD_CAP]}  # already plain text (defensive)
    try:
        raw = binascii.unhexlify(v)
    except (binascii.Error, ValueError):
        return {"hex": v[:_MEMO_FIELD_CAP]}
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"hex": v[:_MEMO_FIELD_CAP], "bytes": len(raw)}
    if any(ord(ch) < 32 and ch not in "\t\n\r" for ch in text):
        return {"hex": v[:_MEMO_FIELD_CAP], "bytes": len(raw)}
    if len(text) > _MEMO_FIELD_CAP:
        return {"text": text[:_MEMO_FIELD_CAP] + "…"}
    return {"text": text}


def memos_of(tx: dict) -> List[dict]:
    """Decoded Memos[] of a tx — [{"type": ..., "format": ..., "data": ...}]."""
    out = []
    for m in (tx.get("Memos") or [])[:_MEMO_COUNT_CAP]:
        if not isinstance(m, dict):
            continue
        f = m.get("Memo") or {}
        entry = {}
        for key, short in (("MemoType", "type"), ("MemoFormat", "format"),
                           ("MemoData", "data")):
            d = _decode_memo_field(f.get(key))
            if d:
                entry[short] = d
        if entry:
            out.append(entry)
    return out


def _tag(tx: dict, pascal: str) -> Optional[int]:
    """Uint32 tag field, PascalCase first with snake_case fallback (defensive)."""
    v = tx.get(pascal)
    if v is None:
        v = tx.get({"DestinationTag": "destination_tag",
                    "SourceTag": "source_tag"}.get(pascal, pascal))
    if isinstance(v, bool) or not isinstance(v, int):
        return None
    return v if 0 <= v <= _TAG_MAX else None


def destination_tag(tx: dict) -> Optional[int]:
    return _tag(tx, "DestinationTag")


def source_tag(tx: dict) -> Optional[int]:
    return _tag(tx, "SourceTag")


# ---------------------------------------------------------------------------
# Amount representation + ledger-truth helpers (byte-parity with filter.py).

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


def drop_int(v) -> Optional[int]:
    """Strict i64 drop integer of an amount representation; None when not one."""
    if isinstance(v, bool):
        return None
    try:
        n = int(str(v), 10)
    except (TypeError, ValueError):
        return None
    return n if 0 <= n < 2 ** 63 else None


def xrp_drops(payload: dict) -> Optional[int]:
    """XRP magnitude of a ghost_payment payload (delivered truth first)."""
    dv = payload.get("delivered")
    if isinstance(dv, dict) and "drops" in dv:
        return drop_int(dv.get("drops"))
    if "drops" in payload:
        return drop_int(payload.get("drops"))
    return None


def currency_display(cur) -> str:
    """Ledger currency value -> bounded read-friendly tag (codenorm decode rule:
    byte-by-byte, stop at the first 0x00 padding byte); 3-char codes pass."""
    if not isinstance(cur, str) or not cur:
        return "?"
    if len(cur) <= 3:
        return cur.upper()
    disp = cur[:6] + "…"  # non-hex / truncated fallback
    if len(cur) == 40 and all(ch in _HEX_CHARS for ch in cur):
        parts = []
        for b in binascii.unhexlify(cur):
            if b == 0:
                break
            parts.append(chr(b) if 32 < b < 127 else f"<{b:02x}>")
        if parts:
            disp = "".join(parts)[:20]
    return disp


def token_tag(payload: dict) -> Optional[str]:
    """Bounded asset tag of a ghost payload: 'XRP' or 'CUR@issuer-suffix'."""
    lim = payload.get("limit")
    if isinstance(lim, dict):
        cur, iss = lim.get("currency"), lim.get("issuer")
    elif "drops" in payload:
        return "XRP"
    else:
        cur, iss = payload.get("currency"), payload.get("issuer")
    if not isinstance(cur, str) or not cur:
        return None
    disp = currency_display(cur)
    return f"{disp}@…{str(iss)[-6:]}" if iss else disp


# ---------------------------------------------------------------------------
# Classification — the ghost lens (all Payments + all TrustSets, nothing else).

def classify_ghost_entry(entry: dict, ghost: str) -> Optional[StructuralHit]:
    """One account_tx entry through the ghost lens -> hit, or None (noise)."""
    tx = meta.entry_tx(entry)
    md = meta.entry_meta(entry)
    if not tx:
        return None
    ledger_index = meta.entry_ledger_index(entry)
    if not ledger_index or ledger_index <= 0:
        return None
    ttype = tx_type_of(tx)
    actor = str(tx.get("Account") or "")
    tx_hash = meta.entry_hash(entry)
    ripple_date = meta.entry_date(entry)

    if ttype == "Payment":
        dest = str(tx.get("Destination") or "")
        if actor == ghost and dest == ghost:
            direction, counterparty = "SELF", ghost
        elif actor == ghost:
            direction, counterparty = "OUT", dest
        elif dest == ghost:
            direction, counterparty = "IN", actor
        else:
            direction, counterparty = "OTHER", dest or actor
        payload = {"class": "ghost_payment", "direction": direction,
                   "counterparty": counterparty, "ghost": ghost,
                   "flags": tx.get("Flags"), **_payment_amount_payload(tx, md),
                   "destination": dest}
        dt, st = destination_tag(tx), source_tag(tx)
        if dt is not None:
            payload["destination_tag"] = dt
        if st is not None:
            payload["source_tag"] = st
        memos = memos_of(tx)
        if memos:
            payload["memos"] = memos
        return StructuralHit(ttype, tx_hash, actor, ledger_index, payload,
                             ripple_date)

    if ttype == "TrustSet":
        la = tx.get("LimitAmount")
        if not amount_is_token(la):
            return None
        issuer = str(la.get("issuer") or "")
        if actor == ghost:
            direction, counterparty = "OUT", issuer
        elif issuer == ghost:
            direction, counterparty = "IN", actor
        else:
            direction, counterparty = "OTHER", issuer or actor
        payload = {"class": "ghost_trustline", "direction": direction,
                   "counterparty": counterparty, "ghost": ghost,
                   "limit": {"currency": la.get("currency"),
                             "issuer": la.get("issuer"), "value": la.get("value")}}
        dt = destination_tag(tx)
        if dt is not None:
            payload["destination_tag"] = dt
        return StructuralHit(ttype, tx_hash, actor, ledger_index, payload,
                             ripple_date)

    return None  # swap/liquidity noise — counted upstream, never recorded