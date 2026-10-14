"""
Lodestone walker CLI — ops + smoke entrypoint (stdlib only).

    cd /home/causeiam/docker-containers/Lodestone
    python3 -m walker --list
    python3 -m walker --token LEDGERGUY --max-pages 120 --dry-run
    python3 -m walker --token LEDGERGUY --force
    python3 -m walker --currency 404 --issuer raHJ4Jz9PYk356wWaDMYw79B17iWtfsSMi
    python3 -m walker --ghost rGhostWallet... [--dry-run|--force|--max-pages N]
    python3 -m walker --ghosts [--dry-run|--force|--max-pages N]

Phase G1 ghost mode (lodestone.md §7) walks ONE account through the ghost lens
(every Payment + TrustSet; direction/counterparty/tag/memo evidence into
ghost_events + a ghost_walks summary row). Ghost mode returns the honest exit
code of the walk: 0 recorded | 1 usage/input | 2 runtime error (ERROR results
included — a failed walk must fail the script calling it).

Phase G3 roster sweep (lodestone.md §13): --ghosts enumerates every
topology_nodes GHOST wallet WITHOUT a COMPLETE ghost_walks dossier (latest
per-ghost row; --force re-walks everything) and walks each sequentially
through the same G1 lens. Batch exit codes mirror --all: 0 when every walk
recorded/OK, 1 on any input error, 2 when any walk ends ERROR (a failed walk
must fail the script calling it).
"""
import argparse
import sys

from . import qdb, xrpl_client
from .config import SEED_TOKENS, SeedToken, WalkerConfig, find_seed, meme_targets
from .ghost import walk_ghost
from .walker import (STATUS_COMPLETE, STATUS_ERROR, STATUS_SKIPPED,
                     WalkResult, walk_token)


def _cfg(args) -> WalkerConfig:
    cfg = WalkerConfig.from_env()
    if args.max_pages:
        cfg.max_pages = args.max_pages
    if args.trustset_limit is not None:
        cfg.trustset_capture_limit = args.trustset_limit
    if args.issuance_limit is not None:
        cfg.issuance_capture_limit = args.issuance_limit
    if args.rpc_timeout:
        cfg.rpc_timeout_s = args.rpc_timeout
    return cfg


def _targets(p, args):
    if args.all:
        return list(meme_targets())
    if args.token:
        s = find_seed(args.token)
        if not s:
            print(f"[x] unknown token {args.token!r} — see --list")
            return None
        return [s]
    if args.currency and args.issuer:
        return [SeedToken(args.currency, args.currency, args.issuer, "adhoc")]
    p.error("choose --token, --currency+--issuer, or --all")


def _run_ghost(args, cfg) -> int:
    """One ghost-account walk (Phase G1) — honest exit codes (docstring)."""
    try:
        rpc = xrpl_client.XrplClient(cfg.rpc_endpoints, cfg.rpc_timeout_s,
                                     cfg.rpc_rate_interval_s, cfg.rpc_max_retries,
                                     cfg.endpoint_down_cooldown_s)
        qcl = None if args.dry_run else qdb.QuestDbClient(
            cfg.qdb_exec_url, cfg.qdb_ilp_url, cfg.qdb_timeout_s, cfg.qdb_max_retries)
        res = walk_ghost(args.ghost, cfg, rpc, qcl, dry_run=args.dry_run,
                         force=args.force, max_pages=cfg.max_pages)
    except ValueError as e:  # invalid ghost account input
        print(f"[x] {e}")
        return 1
    except Exception as e:   # defensive: never crash the CLI
        print(f"[x] ghost walk failed: {type(e).__name__}: {e}")
        return 2
    print("")
    print("=== GHOST WALK SUMMARY ===")
    print(f"  {res.ghost_wallet} | {res.status} | payments={res.payments}"
          f" (in={res.in_payments} out={res.out_payments}"
          f" self={res.self_payments} other={res.other_payments})"
          f" | trustsets={res.trustsets}")
    print(f"  xrp_in={res.xrp_in_drops}d xrp_out={res.xrp_out_drops}d"
          f" | tagged={res.tagged_payments} memo_txs={res.memo_payments}"
          f" | counterparties={res.distinct_counterparties}"
          f" distinct_tags={res.distinct_tags}")
    print(f"  pages={res.pages} txs={res.txs_scanned}"
          f" | events_written={res.events_written}")
    print(f"  linked_tokens: {res.linked_tokens or '-'}")
    print(f"  notes: {res.notes}")
    return 0 if res.status != STATUS_ERROR else 2


def _ghost_roster(q_read, force: bool):
    """Topology GHOST roster + walked-state map (G3 sweep input selection).

    Reads ONLY Lodestone-owned tables: topology_nodes (mapper-discovered
    GHOST nodes) + ghost_walks (latest per-ghost status). COMPLETE dossiers
    are skipped unless --force; PARTIAL/ERROR/NO_ACCOUNT ghosts are
    re-walked (tx_hash ILP dedupe makes re-runs additive, never duplicate)."""
    ds = q_read.dataset("select distinct wallet_address, cluster_id, currency"
                        " from topology_nodes where role='GHOST'")
    roster = [(str(r[0]), str(r[1] or ""), str(r[2] or ""))
              for r in ds if r and r[0]]
    walked = {}
    if roster and not force:
        for r in q_read.dataset("select ghost_wallet, lineage_status from"
                                " ghost_walks LATEST ON walked_at"
                                " PARTITION BY ghost_wallet"):
            if r and r[0]:
                walked[str(r[0])] = str(r[1] or "")
    todo = [row for row in roster
            if force or walked.get(row[0]) != STATUS_COMPLETE]
    return todo, roster, walked


def _run_ghosts(args, cfg) -> int:
    """Phase G3 roster sweep — every topology GHOST lacking a COMPLETE
    dossier walks the G1 lens sequentially (honest batch exit codes)."""
    q_read = qdb.QuestDbClient(cfg.qdb_exec_url, cfg.qdb_ilp_url,
                               cfg.qdb_timeout_s, cfg.qdb_max_retries)
    try:
        todo, roster, walked = _ghost_roster(q_read, args.force)
    except qdb.QdbError as e:
        print(f"[x] ghost roster query failed: {e}")
        return 2
    if not roster:
        print("[x] no GHOST nodes in topology_nodes — run the Phase 3 mapper"
              " first: python3 -m topology")
        return 1
    if not todo:
        print(f"[·] ghost roster complete — all {len(roster)} topology GHOST"
              " wallets hold a COMPLETE dossier (--force re-walks)")
        return 0
    head = ", ".join(w[0][:11] + "…" for w in todo[:6])
    print(f"[·] ghost roster: {len(roster)} discovered | {len(todo)} to walk"
          f" ({head}{' …' if len(todo) > 6 else ''})")
    rpc = xrpl_client.XrplClient(cfg.rpc_endpoints, cfg.rpc_timeout_s,
                                 cfg.rpc_rate_interval_s, cfg.rpc_max_retries,
                                 cfg.endpoint_down_cooldown_s)
    qcl = None if args.dry_run else q_read
    rc, rows = 0, []
    for wallet, cluster, currency in sorted(todo, key=lambda r: (r[1], r[0])):
        short = wallet[:12] + "…"
        tag = "re-walk" if walked.get(wallet) else "new"
        try:
            res = walk_ghost(wallet, cfg, rpc, qcl, dry_run=args.dry_run,
                             force=args.force, max_pages=cfg.max_pages)
        except ValueError as e:  # invalid account input — keep sweeping
            print(f"[x] {short} [{tag}]: {e}")
            rows.append((short, cluster[:13], currency or "-", "INPUT_ERROR",
                         "-", str(e)[:44]))
            rc = max(rc, 1)
            continue
        except Exception as e:  # defensive: the sweep must survive each walk
            print(f"[x] {short} [{tag}]: {type(e).__name__}: {e}")
            rows.append((short, cluster[:13], currency or "-", STATUS_ERROR,
                         "-", f"{type(e).__name__}: {e}"[:44]))
            rc = max(rc, 2)
            continue
        print(f"[·] {short} [{tag}] {res.status} pages={res.pages}"
              f" txs={res.txs_scanned} payments={res.payments}"
              f" memos={res.memo_payments} events={res.events_written}")
        rows.append((short, cluster[:13], currency or "-", res.status,
                     f"ev+{res.events_written}", res.notes[:46]))
        if res.status == STATUS_ERROR:
            rc = max(rc, 2)
    print("")
    print("=== GHOST ROSTER WALK SUMMARY ===")
    for r in rows:
        print("  " + " | ".join(str(x) for x in r))
    return rc


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="walker",
        description="Lodestone forensic ledger walker (Phase 2 + G1 ghost)")
    p.add_argument("--list", action="store_true", help="print the seed registry and exit")
    p.add_argument("--token", help="seed display name (e.g. LEDGERGUY)")
    p.add_argument("--currency", help="raw currency code (3-char or 40-hex)")
    p.add_argument("--issuer", help="issuer classic address (r...)")
    p.add_argument("--all", action="store_true", help="walk every meme seed sequentially")
    p.add_argument("--ghost", help="walk ONE account with the ghost lens (Phase G1)")
    p.add_argument("--ghosts", action="store_true",
                   help="Phase G3: walk every topology GHOST wallet lacking "
                        "a COMPLETE ghost_walks dossier")
    p.add_argument("--dry-run", action="store_true", help="no QuestDB writes")
    p.add_argument("--force", action="store_true",
                   help="re-walk past a cached COMPLETE row (tx_hash dedupe)")
    p.add_argument("--max-pages", type=int, help="forward-walk page budget override")
    p.add_argument("--trustset-limit", type=int)
    p.add_argument("--issuance-limit", type=int)
    p.add_argument("--rpc-timeout", type=float)
    args = p.parse_args(argv)
    cfg = _cfg(args)

    if args.list:
        print(f"{'display':<14} {'currency':<14} {'issuer':<36} category")
        for t in SEED_TOKENS:
            print(f"{t.display:<14} {t.currency:<14} {t.issuer:<36} {t.category}")
        return 0

    if args.ghost:  # Phase G1 — mutually exclusive with the token-selection flags
        if args.token or args.currency or args.issuer or args.all:
            p.error("--ghost walks one account; do not combine with "
                    "--token/--currency/--issuer/--all")
        return _run_ghost(args, cfg)

    if args.ghosts:  # Phase G3 roster sweep — mutually exclusive inputs
        if (args.token or args.currency or args.issuer or args.ghost
                or args.all):
            p.error("--ghosts sweeps the topology roster; do not combine with"
                    " --ghost/--token/--currency/--issuer/--all")
        return _run_ghosts(args, cfg)

    targets = _targets(p, args)
    if targets is None:
        return 1

    rpc = xrpl_client.XrplClient(cfg.rpc_endpoints, cfg.rpc_timeout_s,
                                 cfg.rpc_rate_interval_s, cfg.rpc_max_retries,
                                 cfg.endpoint_down_cooldown_s)
    qcl = None if args.dry_run else qdb.QuestDbClient(
        cfg.qdb_exec_url, cfg.qdb_ilp_url, cfg.qdb_timeout_s, cfg.qdb_max_retries)

    rc, rows = 0, []
    for t in targets:
        try:
            res = walk_token(t.currency, t.issuer, cfg, rpc, qcl,
                             dry_run=args.dry_run, force=args.force,
                             max_pages=cfg.max_pages)
        except ValueError as e:  # invalid issuer/currency input
            print(f"[x] {t.display}: {e}")
            rows.append((t.display, "INPUT_ERROR", "-", "-", "-", str(e)))
            rc = max(rc, 1)
            continue
        except Exception as e:  # defensive: the CLI loop must survive every walk
            res = WalkResult(t.currency, t.issuer, status=STATUS_ERROR,
                             notes=f"{type(e).__name__}: {e}")
            rc = max(rc, 2)
        rows.append((t.display, res.status, res.ghost_wallet or "-",
                     res.amm_account or "-", f"{res.confidence:.2f}", res.notes))
    print("")
    print("=== WALK SUMMARY ===")
    for r in rows:
        print("  " + " | ".join(str(x)[:70] for x in r))
    return 0 if not args.all else rc  # sequential multi-walks fail loudly


if __name__ == "__main__":
    sys.exit(main())
