"""
Lodestone — address & currency canonicalization (stdlib, no deps).

Currency handling keeps byte-parity with the trading-bot's AssetWhitelist.java:
  * 3-char codes pass through untouched; 40-char [0-9A-Fa-f] inputs pass through
    (idempotency guard); other values are hex-encoded per-character.
  * Decoding is byte-by-byte and stops at the first 0x00 padding byte — the exact
    no-trailing-zero-strip rule documented at AssetWhitelist.java lines 47-64
    (stripping would corrupt 'P' = 0x50 byte pairs).
Divergence note (documented AssetWhitelist edge): Java Integer.toHexString()
does not zero-pad a sub-0x10 char; this module zero-pads to full bytes so the
encoding is byte-aligned like rippled's wire format. Identical for every ASCII
whitelist currency; non-ASCII currency codes are out of whitelist scope.

Also owns: XRPL classic-address checksum validation and ripple-epoch math
(Ripple epoch = seconds since 2000-01-01; offset to unix = +946684800).
"""
import hashlib
import datetime as _dt

BASE58_ALPHABET = "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz"
_B58_INDEX = {c: i for i, c in enumerate(BASE58_ALPHABET)}
RIPPLE_EPOCH_OFFSET = 946684800  # unix(1970) -> ripple-epoch(2000-01-01)


def _b58decode(s: str) -> bytes:
    acc = 0
    for ch in s:
        i = _B58_INDEX.get(ch)
        if i is None:
            raise ValueError(f"invalid XRPL base58 character: {ch!r}")
        acc = acc * 58 + i
    body = acc.to_bytes((acc.bit_length() + 7) // 8, "big") if acc else b""
    zeros = len(s) - len(s.lstrip("r"))  # 'r' maps to value 0 in the XRPL alphabet
    return b"\x00" * zeros + body


def validate_classic_address(addr: str) -> bool:
    """True iff `addr` is a well-formed XRPL classic address (checksum verified)."""
    try:
        raw = _b58decode(addr)
    except (ValueError, AttributeError):
        return False
    if len(raw) != 25 or raw[0] != 0:
        return False
    return hashlib.sha256(hashlib.sha256(raw[:21]).digest()).digest()[:4] == raw[21:25]


def normalize_issuer(addr: str) -> str:
    """Validate + normalize an issuer/actor address; raises ValueError when bad."""
    a = (addr or "").strip()
    if not validate_classic_address(a):
        raise ValueError(f"invalid XRPL classic address: {addr!r}")
    return a


def currency_display_to_hex(currency: str) -> str:
    """Display form -> 40-char uppercase wire hex (3-char codes returned as-is)."""
    c = (currency or "").strip()
    if not c:
        raise ValueError("empty currency code")
    if len(c) <= 3 and c.isascii() and c.isalnum():
        return c.upper()
    if len(c) == 40 and all(ch in "0123456789abcdefABCDEF" for ch in c):
        return c.upper()
    if not c.isascii():
        raise ValueError(f"non-ASCII currency code out of whitelist scope: {c!r}")
    out = "".join(f"{ord(ch):02x}" for ch in c)
    return (out + "0" * (40 - len(out)))[:40].upper()


def currency_matches(value, display: str, hex40: str) -> bool:
    """True iff RPC-side currency `value` denotes the same asset.

    rippled returns standard 3-char codes as ASCII and custom codes as 40-hex;
    targets must match both shapes (whitelist stores display, ledger carries hex).
    """
    if not isinstance(value, str) or not value:
        return False
    if len(value) == 40 and value.upper() == hex40.upper():
        return True
    return value.strip().upper() == display.upper()


def ripple_date_to_unix(d) -> int:
    """Ripple-epoch seconds (ledger/tx `date`) -> unix epoch seconds."""
    return int(d) + RIPPLE_EPOCH_OFFSET


def ripple_date_to_iso(d) -> str:
    """Ripple-epoch seconds -> ISO-8601 UTC string (log/UI friendly)."""
    return _dt.datetime.fromtimestamp(ripple_date_to_unix(d), _dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def is_tx_hash(v: str) -> bool:
    """64-char hex SHA-512Half transaction hash check."""
    return isinstance(v, str) and len(v) == 64 and all(
        ch in "0123456789abcdefABCDEF" for ch in v)