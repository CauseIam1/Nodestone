"""
Lodestone Walker — configuration & whitelisted seed registry.

Blueprint source of truth: /docker-containers/Lodestone/lodestone.md §3 (Forensic
Engine & Ledger Walker) + §4 (Multi-Account Topology Mapping). Schema contract:
Lodestone/lodestone_init.sql (Phase 1; byte-identical init.d copy at
questdb/init/04-lodestone-lineage-schema.sql).

Isolation Rule (lodestone.md §1 + schema headers): this engine writes ONLY the
Lodestone-owned forensic tables — token_lineage / forensic_audit_events (issuer
lens) and ghost_walks / ghost_events (§7 ghost lens) — never amm_balances,
mpt_state_snapshot, asset_rotation_matrix, trade_events, sweep_locks, amm_pools,
wallet_registry.

Deployment duality: host smoke/ops runs use localhost URLs; the Phase 4 container
deployment overrides via LODESTONE_* env vars. The RPC list puts the local
private rippled first with public full-history archive fallback (blueprint §3);
an unreachable local endpoint degrades via endpoint cooldown — graceful.
Stdlib only.
"""
import os
from dataclasses import dataclass
from typing import Optional

# Live-verified 2026-10-10: xrplcluster complete_ledgers=32570-107567882
# (full mainnet history, server_state=full) — the archive workhorse.
# s2 reachable but range-clamped; s1 short-window only.
DEFAULT_RPC_ENDPOINTS = (
    "http://localhost:5005",        # our node (Phase 4 container path: http://rippled:5005)
    "https://xrplcluster.com",      # full-history archive 32570..now
    "https://s2.ripple.com",        # Ripple archive (clamped range)
    "https://s1.ripple.com",        # Ripple short-window fallback
)

DEFAULT_QDB_EXEC_URL = "http://localhost:9000/exec"               # SQL reads (Phase 1 path)
DEFAULT_QDB_ILP_URL = "http://localhost:9000/write?precision=ns"  # ILP writes (probed 204)


@dataclass
class WalkerConfig:
    """All walk knobs. Defaults are conservative for public-node courtesy."""

    rpc_endpoints: tuple = DEFAULT_RPC_ENDPOINTS
    rpc_timeout_s: float = 20.0
    rpc_rate_interval_s: float = 0.3          # min gap between ANY two RPC calls
    rpc_max_retries: int = 2                  # full endpoint cycles per request
    endpoint_down_cooldown_s: float = 120.0

    account_tx_page_size: int = 200           # archive nodes clamp to their cap
    max_pages: int = 4000                     # forward-walk page budget (safety valve)
    forward_first: bool = True                # §3 pagination, oldest-first (walker.py)

    trustset_capture_limit: int = 25          # §3: earliest holder trust networks
    issuance_capture_limit: int = 25          # earliest issuance Payments kept
    accountset_capture_limit: int = 10        # earliest issuer config events kept
    genesis_issuance_window_ledgers: int = 1440  # ~1h, first-issuance proximity bonus

    qdb_exec_url: str = DEFAULT_QDB_EXEC_URL
    qdb_ilp_url: str = DEFAULT_QDB_ILP_URL
    qdb_timeout_s: float = 10.0
    qdb_max_retries: int = 3

    ghost_dedupe_scan_limit: int = 200_000  # §7 resume dedupe tx_hash pre-scan cap
    ghost_roundtrip_window_ledgers: int = 1440  # §8 exact-amount relay window (~96 min)
    ghost_crossref_event_cap: int = 100_000  # §8 bounded per-ghost ghost_events read

    @staticmethod
    def from_env() -> "WalkerConfig":
        """LODESTONE_* overrides — wired for the Phase 4 container deployment."""
        cfg = WalkerConfig()
        for attr, cast in (
            ("qdb_exec_url", str), ("qdb_ilp_url", str),
            ("rpc_timeout_s", float), ("rpc_rate_interval_s", float),
            ("rpc_max_retries", int), ("endpoint_down_cooldown_s", float),
            ("account_tx_page_size", int), ("max_pages", int),
            ("trustset_capture_limit", int), ("issuance_capture_limit", int),
            ("accountset_capture_limit", int),
            ("genesis_issuance_window_ledgers", int),
            ("qdb_timeout_s", float), ("qdb_max_retries", int),
            ("ghost_dedupe_scan_limit", int),
            ("ghost_roundtrip_window_ledgers", int),
            ("ghost_crossref_event_cap", int),
        ):
            v = os.environ.get("LODESTONE_" + attr.upper())
            if v not in (None, ""):
                try:
                    setattr(cfg, attr, cast(v) if cast is not str else v)
                except (TypeError, ValueError):
                    pass
        eps = os.environ.get("LODESTONE_RPC_ENDPOINTS")
        if eps:
            cfg.rpc_endpoints = tuple(e.strip() for e in eps.split(",") if e.strip())
        return cfg

# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SeedToken:
    """One whitelisted walk target, mirrored VERBATIM from AssetWhitelist.java."""

    display: str   # canonical stored SYMBOL form — mirrors the Java raw string
    currency: str  # exactly the AssetWhitelist.java argument (pre-canon)
    issuer: str    # classic r-address
    category: str  # "meme" (default targets) or "other" (stable/utility/bridge)


# Parity mirror of trading-bot AssetWhitelist.java lines 82-104 ("Your Monitored
# Pools") + lines 96-112 additions. XRP intentionally absent: the base fabric asset
# has no issuer/issued token (fiat/stable rule: pass-through nodes, never capital).
SEED_TOKENS: tuple = (
    SeedToken("404", "404", "raHJ4Jz9PYk356wWaDMYw79B17iWtfsSMi", "meme"),
    SeedToken("$TRUMP", "$TRUMP", "r3iM2Ffe9Krgn6n3qhHj2oe8kiJMKB63s7", "meme"),
    SeedToken("XAH", "XAH", "rswh1fvyLqHizBS2awu1vs6QcmwTBd9qiv", "meme"),
    SeedToken("ATM", "ATM", "raDZ4t8WPXkmDfJWMLBcNZmmSHmBC523NZ", "meme"),
    SeedToken("MELANIA", "MELANIA", "rsqo1sPRFE2uXgAWWzoXiD35ZwRmHv78dC", "meme"),
    SeedToken("SGB", "SGB", "rctArjqVvTHihekzDeecKo6mkTYTUSBNc", "meme"),
    SeedToken("X", "X", "ruxT12tH2mrccHFDjnjqWyGfAxYxYMHLA", "meme"),
    SeedToken("BARRONTRUMP", "BARRONTRUMP", "rLQjAd5o5XrzLTW1SxHDLL9pwLTyti159E", "meme"),
    SeedToken("LEDGERGUY", "LEDGERGUY", "rEUo2P65TfnhksXuietsjxUFCyYiiK83F5", "meme"),
    SeedToken("BEAR", "BEAR", "rBEARGUAsyu7tUw53rufQzFdWmJHpJEqFW", "meme"),
    SeedToken("Hand", "Hand", "rssZy6J98oa9iYatGx5CcoTJPCnWTvL5Bg", "meme"),
    SeedToken("LedgerGirl", "LedgerGirl", "r4h8BcTV9WkLWd1PqeHEhwPfHdBm7DinJ4", "meme"),
    SeedToken("EUROP", "EUROP", "rMkEuRii9w9uBMQDnWV5AA43gvYZR9JxVK", "other"),
    SeedToken("BBRL", "BBRL", "rH5CJsqvNqZGxrMyGaqLEoMWRYcVTAPZMt", "other"),
    SeedToken("AUDD", "AUDD", "rUN5Zxt3KAnMRJgEWywDJT8QDMMeLH5ok", "other"),
    SeedToken("FLR", "FLR", "rcxJwVnftZzXqyH9YheB8TgeiZUhNo1Eu", "other"),
    SeedToken("USDC", "USDC", "rcEGREd8NmkKRE8GE424sksyt1tJVFZwu", "other"),
    SeedToken("RLUSD", "RLUSD", "rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De", "other"),
    SeedToken("USDT", "USDT", "rcEGREd8NmkKRE8GE424sksyt1tJVFZwu", "other"),
)


# Operator ruling (2026-10-10): XAH, SGB, and FLR are EXCLUDED from the
# forensic walk — the --all batch and every retry sweep skip them outright
# (XAH flaked against the public archive mid-walk; operator elected to
# descope). SEED_TOKENS stays the verbatim AssetWhitelist.java parity
# mirror — the registry keeps these rows, they are simply never walked,
# so no lineage row ever materializes for them. A named walk
# (`walker --token SGB`) remains possible if the ruling is ever reversed.
WALK_EXCLUDED: tuple = ("XAH", "SGB", "FLR")


def meme_targets() -> tuple:
    """Default forensic target set — the whitelisted meme pile, minus the
    standing operator walk-exclusions (XAH / SGB / FLR)."""
    return tuple(
        t for t in SEED_TOKENS
        if t.category == "meme" and t.display not in WALK_EXCLUDED
    )


def find_seed(query: str) -> Optional[SeedToken]:
    """Lookup by display (case-insensitive) or exact currency string."""
    q = (query or "").strip()
    if not q:
        return None
    for t in SEED_TOKENS:
        if t.display.casefold() == q.casefold():
            return t
    for t in SEED_TOKENS:
        if t.currency == q:
            return t
    return None