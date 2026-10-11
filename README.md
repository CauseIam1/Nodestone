# Nodestone
## Project Lodestone — XRPL Forensic Tracer & Token Lineage Engine
Document Status: ARCHITECTURE SPECIFICATION — HUD AS-BUILT & LIVE-VERIFIED (Oct 10, 2026)
### This document defines the overarching vision, database schemas, forensic algorithms, and UI integration rules for Lodestone, the specialized XRPL historical lineage and creator-tracking engine for Project North Star.
## 1. System Purpose & Architectural Philosophy
The live trading bot ecosystem operates on high-velocity real-time execution (MPT_RPN_WALLET_ADDRESS and the live AMM mesh). Lodestone is a strictly decoupled forensic microservice designed to pierce through high-volume AMM swap noise and trace meme coin origins back to their true deployers and funding sources.
 * Primary Objective: Map multi-account deployment lineages, identify issuer "ghost" funding wallets, and calculate creator confidence scores without creating I/O contention on the live trading engine.
 * The Isolation Rule: Lodestone never shares write paths or execution loops with active trading strategies (Mean Reversion, Four-Hop Atomic, etc.). It utilizes isolated database tables and public full-history RPC fallbacks.
 * Write Once, Cache Forever: Because token deployment histories are immutable, Lodestone walks the historical ledger once per token, commits the resolved relationship tree to QuestDB, and serves subsequent UI queries instantly from cache.
## 2. Database Architecture (QuestDB Integration)
Lodestone persists its data in the existing QuestDB instance via the PostgreSQL wire protocol (port 8812), utilizing two dedicated, purpose-built tables separated entirely from live operational metrics (amm_balances, mpt_state_snapshot).
### A. The Summary Table: token_lineage
Stores the final resolved origin story of each whitelisted asset for instant GUI retrieval.
CREATE TABLE token_lineage (
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

### B. The Step-by-Step Table: forensic_audit_events
Logs the granular chronological milestone events discovered during historical ledger walks.
CREATE TABLE forensic_audit_events (
    timestamp TIMESTAMP,
    currency SYMBOL CAPACITY 256 CACHE,
    issuer SYMBOL CAPACITY 100 CACHE,
    tx_hash SYMBOL CAPACITY 64 CACHE,
    tx_type SYMBOL CAPACITY 32 CACHE, -- e.g., 'AMMCreate', 'TrustSet', 'Payment', 'AccountSet'
    actor_address SYMBOL CAPACITY 100 CACHE,
    ledger_index LONG,
    payload_json STRING
) TIMESTAMP(timestamp) PARTITION BY MONTH WAL;

## 3. Forensic Engine & Ledger Walker Specification
### Because local rippled nodes maintain a restricted operational window (e.g., ~2,000 to 4,000 recent ledgers), Lodestone's backend engine incorporates specialized crawling logic:
 * Remote Full-History Fallback: Automatically routes historical requests (account_tx, ledger) to robust public full-history archive nodes when querying past genesis blocks.
 * Pagination & Continuation: Implements recursive loop handling utilizing XRPL marker fields to page completely through an account's transactional history back to its inception.
 * Target Transaction Filters: Explicitly ignores routine swap and liquidity churn, filtering raw ledger streams specifically for structural transaction classes:
   * AMMCreate: Establishes initial pool liquidity and day-zero liquidity providers.
   * TrustSet: Maps early token distribution networks and initial holder interactions.
   * Payment (Genesis): Identifies initial reserve funding transfers covering account creation costs.
   * AccountSet: Captures issuer configuration adjustments (transfer fees, flags like DefaultRipple).
 * Metadata Parsing: Inspects transaction metadata (AffectedNodes), specifically parsing CreatedNode entries for new ledger objects and PreviousFields for reserve adjustments.
## 4. Multi-Account Topology Mapping
### Lodestone correlates three distinct layers of account architecture to expose hidden deployers:
 * The Issuer Account: The designated minting address holding custom currency definitions.
 * The Funding "Ghost" Wallet: The external origin account that initially supplied XRP reserves to the issuer. Cross-referencing ghost wallets across multiple tokens exposes repeat creators.
 * The AMM Pool Account: The pseudo-account derived by the ledger for liquidity routing, linked directly to its day-zero creators.
## 5. UI & Holodeck Integration — The Lodestone HUD (as built Oct 10, 2026)
### Implemented as a dedicated, read-only page inside the North Star Holodeck dashboard (trading-dashboard container, Next.js 14 App Router, port 3000), matching the high-contrast, dark glassmorphic aesthetic of the LGI HUD and North Star Holodeck. The HUD renders complex forensic data intuitively:
 * The Constellation Matrix (Relationship Visualizer): src/app/lodestone/Constellation.tsx renders an interactive dependency tree placing the Issuer token at the center, branching backward to the Ghost Funding Wallet and forward to the AMM Pool. Node and edge colors come from the engine vocabularies in lodestone-types.ts (ROLE_DOT / EDGE_COLOR: amber ISSUER, violet GHOST, cyan AMM_CREATOR, green AMM_POOL; GENESIS_FUNDING / AMM_CREATE emphasize day-zero structure), and the ClusterChips surface cross-token clusters, shared-ghost clusters, and the active currency as matched neon tags. The canvas mounts only once the walker has committed a graph for the token (committed: true), showing an "establishing bridge …" / "constellation not committed yet · walker still walking" empty state otherwise.
 * Token Dossier & Confidence Meter (src/app/lodestone/panels.tsx): asset metadata with token-accent coloring, the discovered_at date, AddressRows for issuer / ghost·genesis / AMM pool (live token_lineage row as primary, cluster-derived addresses as fallbacks), NotesChips, the futuristic radial ConfidenceGauge representing the Creator Confidence Score (calculated from funding signatures and historical cluster analysis), and a StatusChip sourced from token_lineage.lineage_status. Chip colors render only from the verified vocabulary — any status outside the known set (e.g. GENESIS_ONLY) renders PROCESSING cyan: the HUD never paints an unverified claim green.
 * Chronological Audit Stream (The Black Box): src/app/lodestone/panels.tsx AuditStream outputs the selected token's forensic_audit_events milestones step-by-step (e.g., [✓] Traced reserve funding payment back to Ghost Wallet...).
 * Frontend Access: the LODESTONE tab in the Holodeck nav ribbon (src/app/layout.tsx) routes to /lodestone (page.tsx — three zones: stream strip, constellation, dossier + black box). Data bridge — src/app/api/lodestone/route.ts: GET /api/lodestone?view=summary | graph | audit &token=<currency>&limit=n → pg pool → QuestDB questdb:8812 (database qdb). Isolation is structural: the handler is a pure read window with no browser-reachable write path to token_lineage or forensic_audit_events; the engine side (Python ledger walker + topology GraphBuilder on the Mainframe host) owns the only insert path and writes QuestDB directly.
 * Page blueprint: /docker-containers/trading-dashboard/src/app/lodestone/LodestonePage.md
6. As-Built Verification Log (Oct 10, 2026)
 * Vocabulary discipline: lodestone-types.ts contains nothing invented — payload contracts mirror live-verified QuestDB column names, and the role/edge enums are authored by the engine itself (roles & edge types: Lodestone/topology/graph.py GraphBuilder; 12-meme seed roster: SEED_TOKENS in Lodestone/walker/config.py, mirrored verbatim from trading-bot AssetWhitelist.java (default walk set is `meme_targets()` — these rows minus the standing `WALK_EXCLUDED` = XAH/SGB/FLR descope of mainstream network tokens, operator ruling Oct 10, 2026; registry rows stay verbatim); the roster's stable/bridge roots remain pass-through settlement nodes, never capital).
 * Type integrity: in-container tsc --noEmit (trading-dashboard, node_modules/.bin/tsc) → exit 0, zero errors across src/.
 * Live smoke (curl, port 3000): view=summary → cluster C398aeb6ba56b (LEDGERGUY): ISSUER 1 / GHOST 1 / HOLDER 26 (28 nodes / 27 edges), lineage_status GENESIS_ONLY, creator_confidence 0.6, audit_total 53; graph?token=LEDGERGUY → committed: true with the genesis funder resolved (142.8 XRP drops); audit?token=LEDGERGUY → Payment / token-return milestones; /lodestone → HTTP 200.
 * Delivery: trading-dashboard bind-mounts ./trading-dashboard:/app — source hot-loads on port 3000 (an image rebuild is optional, only to bake the change into the image).
