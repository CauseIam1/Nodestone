-- ============================================================================
-- Lodestone/lodestone_init.sql — QuestDB forensic lineage schema (CANONICAL)
-- ============================================================================
-- Source of truth: Lodestone/lodestone.md (architecture spec), Section 2 —
-- "Database Architecture (QuestDB Integration)". DDL below is the blueprint
-- verbatim, plus CREATE TABLE IF NOT EXISTS (proven supported on this stack:
-- used by DatabaseService.java and questdb/init/02-bootstrap-live-positions.sql).
--
-- Deployment paths:
--   1. LIVE stack (running questdb container, NO restart): provision one-shot,
--      statement-by-statement, via the QuestDB HTTP console (:9000/exec).
--   2. FRESH stacks: SAME one-shot path — the ./questdb/init init.d mount is
--      INERT (QuestDB Community 10.0.1 never executes conf/init.d scripts;
--      verified Oct 14 2026 via image entrypoint + recreate boot logs). The
--      mount merely keeps the deployed copy inside the container for
--      reference. Recipe + receipts: lodestone.md §9/§10.
--   NOTE: keep the two files in sync — schema must stay byte-equivalent
--   (only this header banner differs between them).
--
-- ISOLATION RULE: both tables are owned exclusively by the Lodestone forensic
-- microservice (read-only public XRPL RPC + writes only to these tables).
-- They are NEVER written by the live trading-bot or myXRPL engines — forensic
-- history must never touch live trading operational metrics (amm_balances,
-- mpt_state_snapshot, asset_rotation_matrix, trade_events, sweep_locks).
--
-- QuestDB 8.x reader grammar (northstar.md): collapse to the latest lineage
-- version per asset with LATEST ON — bare LATEST BY is REJECTED on this stack:
--   SELECT * FROM token_lineage
--   LATEST ON discovered_at PARTITION BY issuer, currency;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- A. token_lineage — The Summary Table
-- Stores the final resolved origin story of each whitelisted asset for instant
-- GUI retrieval. Every COMPLETED walk APPENDS a new row with a fresh
-- discovered_at timestamp (versioning); the GUI always reads the LATEST
-- ON-collapsed view above.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS token_lineage (
    discovered_at TIMESTAMP,
    currency SYMBOL CAPACITY 256 CACHE,
    issuer SYMBOL CAPACITY 100 CACHE,
    funding_wallet SYMBOL CAPACITY 100 CACHE,
    amm_account SYMBOL CAPACITY 100 CACHE,
    amm_create_ledger LONG,
    creator_confidence DOUBLE,
    lineage_status SYMBOL CAPACITY 16 CACHE,
    notes STRING
) TIMESTAMP(discovered_at) PARTITION BY DAY WAL;

-- ----------------------------------------------------------------------------
-- B. forensic_audit_events — The Step-by-Step Table
-- Logs granular chronological milestone events discovered during historical
-- ledger walks (Black Box feed for the Lodestone HUD terminal).
-- tx_type filters for structural transaction classes only:
--   AMMCreate (day-zero pool liquidity), TrustSet (early holder network),
--   Payment/genesis (ghost reserve funding), AccountSet (issuer config).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS forensic_audit_events (
    timestamp TIMESTAMP,
    currency SYMBOL CAPACITY 256 CACHE,
    issuer SYMBOL CAPACITY 100 CACHE,
    tx_hash SYMBOL CAPACITY 64 CACHE,
    tx_type SYMBOL CAPACITY 32 CACHE,
    actor_address SYMBOL CACHE,
    ledger_index LONG,
    payload_json STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL;

-- ============================================================================
-- Phase 3 — Multi-Account Topology Mapping (lodestone.md Section 4)
-- ============================================================================
-- Produced by the Phase 3 topology mapper. Append-only versioning, same
-- Write-Once-Cache-Forever model; readers collapse with LATEST ON:
--   SELECT * FROM topology_nodes
--   LATEST ON mapped_at PARTITION BY cluster_id, wallet_address, role;
--   SELECT * FROM topology_edges
--   LATEST ON mapped_at PARTITION BY cluster_id, currency, edge_type,
--             from_address, to_address;
-- CROSSREF SOURCES (strictly read-only; NEVER written by this engine):
--   amm_pools (mesh pool_account links) + wallet_registry (internal flags).
-- cluster_id = "C" + sha256(ghost_wallet)[:12] — identical ghost wallets
-- reused across issuers collapse into ONE cluster (repeat-creator detect).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- C. topology_nodes — Constellation nodes: Issuer | Ghost | Holder |
--    AMM_CREATOR | AMM_POOL, with first-evidence ledger + compact payload.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS topology_nodes (
    mapped_at TIMESTAMP,
    cluster_id SYMBOL CAPACITY 64 CACHE,
    wallet_address SYMBOL CACHE,
    role SYMBOL CACHE,
    currency SYMBOL CACHE,
    issuer SYMBOL CACHE,
    ledger_index LONG,
    payload_json STRING
) TIMESTAMP(mapped_at) PARTITION BY DAY WAL;

-- ----------------------------------------------------------------------------
-- D. topology_edges — Mapped relationships with tx anchors + edge confidence
--    (GENESIS_FUNDING .60 | ISSUANCE .30 | TRUST .20 | AMM_CREATE .80 |
--     POOL_MESH .90 mesh-registry link; mesh links are never creator proofs).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS topology_edges (
    mapped_at TIMESTAMP,
    cluster_id SYMBOL CAPACITY 64 CACHE,
    currency SYMBOL CACHE,
    issuer SYMBOL CACHE,
    edge_type SYMBOL CACHE,
    from_address SYMBOL CACHE,
    to_address SYMBOL CACHE,
    ledger_index LONG,
    tx_hash SYMBOL CAPACITY 64 CACHE,
    edge_confidence DOUBLE,
    payload_json STRING
) TIMESTAMP(mapped_at) PARTITION BY MONTH WAL;

-- ============================================================================
-- Phase G1 — Ghost Wallet Forensic Walk (lodestone.md Section 7, Oct 14, 2026)
-- ============================================================================
-- ONE walked account — the genesis-funding GHOST wallet — under the lens:
-- every Payment it ever sent or received (XRP or any IOU) plus every TrustSet
-- it signed, captured as wallet-behavior evidence for Phase G2 venue
-- correlation and constellation edges. Same ISOLATION RULE as tables A-D:
-- owned exclusively by the Lodestone forensic engine; NEVER written by the
-- live trading-bot or myXRPL engines.
-- Ghost-relative DIRECTION semantics: OUT ghost sent | IN ghost received |
--   SELF both sides are the ghost | OTHER pathway involvement on neither side
--   (defensive keep — no evidence silently dropped).
-- Readers collapse each ghost's walk versions with LATEST ON:
--   SELECT * FROM ghost_walks
--   LATEST ON walked_at PARTITION BY ghost_wallet;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- E. ghost_walks — The Ghost Summary Table (append-only versioning)
-- One appended row per completed walk. XRP sums register delivered_amount
-- ledger truth, never the Amount envelope (house rule).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ghost_walks (
    walked_at TIMESTAMP,
    ghost_wallet SYMBOL CAPACITY 100 CACHE,
    lineage_status SYMBOL CAPACITY 16 CACHE,
    first_ledger LONG,
    last_ledger LONG,
    txs_scanned LONG,
    payment_count LONG,
    trustset_count LONG,
    tagged_payment_count LONG,
    memo_payment_count LONG,
    distinct_counterparties LONG,
    distinct_tags LONG,
    xrp_in_drops LONG,
    xrp_out_drops LONG,
    linked_tokens STRING,
    notes STRING
) TIMESTAMP(walked_at) PARTITION BY DAY WAL;

-- ----------------------------------------------------------------------------
-- F. ghost_events — Ghost transaction evidence (one row per captured tx)
-- counterparty = the OTHER side relative to the ghost; destination_tag is the
-- per-user deposit fingerprint consumed by Phase G2 venue correlation.
-- payload_json classes: ghost_payment | ghost_trustline (amounts + delivered
-- truth, Flags incl. tfPartialPayment, hex-decoded Memos[], SourceTag).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ghost_events (
    timestamp TIMESTAMP,
    ghost_wallet SYMBOL CAPACITY 100 CACHE,
    tx_hash SYMBOL CAPACITY 64 CACHE,
    tx_type SYMBOL CAPACITY 32 CACHE,
    direction SYMBOL CAPACITY 16 CACHE,
    counterparty SYMBOL CACHE,
    destination_tag LONG,
    ledger_index LONG,
    payload_json STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL;

-- ============================================================================
-- Phase G2 — Ghost Venue Crossref (lodestone.md Section 8, Oct 14, 2026)
-- ============================================================================
-- Correlates ALL walked ghosts' §7 dossiers into per-ghost venue attributions
-- and same-exit clusters, derived from ledger evidence ONLY — never hardcoded
-- exchange address lists. Three §8 signal classes: DestinationTag deposit
-- fingerprints (tagged OUT Payments), cross-ghost reuse of hex-decoded memo
-- identities, and exact-amount round-trips through a shared relay account
-- (delivered_amount truth; A != B enforced; ±window ledgers).
-- One appended row per ghost x venue per attribution run (versioning);
-- readers collapse each ghost-venue pair with LATEST ON:
--   SELECT * FROM ghost_venues
--   LATEST ON attributed_at PARTITION BY ghost_wallet, venue_address;
-- Same ISOLATION RULE as tables A-F: owned exclusively by the Lodestone
-- forensic engine; NEVER written by the live trading-bot or myXRPL engines.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- G. ghost_venues — Ghost venue attribution + same-exit clusters
-- venue_label is ledger-derived (memo identity | tag fingerprint | relay),
-- never a hardcoded venue name. evidence_score follows the deterministic §8
-- rubric (clamp 0.98 — absolute ledger proof stays reserved). crossref_ghosts
-- records OTHER walked ghosts sharing the venue (the same-exit cluster);
-- notes carries the evidence census (the skip-compare key for idempotence).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ghost_venues (
    attributed_at TIMESTAMP,
    ghost_wallet SYMBOL CAPACITY 100 CACHE,
    venue_address SYMBOL CACHE,
    venue_label STRING,
    evidence_score DOUBLE,
    tagged_out_count LONG,
    distinct_tags LONG,
    memo_event_count LONG,
    memo_fingerprints STRING,
    roundtrip_count LONG,
    crossref_ghosts STRING,
    notes STRING
) TIMESTAMP(attributed_at) PARTITION BY DAY WAL;