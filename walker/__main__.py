"""
Lodestone walker CLI — Phase 2 ops + smoke entrypoint (stdlib only).

    cd /home/causeiam/docker-containers/Lodestone
    python3 -m walker --list
    python3 -m walker --token LEDGERGUY --max-pages 120 --dry-run
    python3 -m walker --token LEDGERGUY --force
    python3 -m walker --currency 404 --issuer raHJ4Jz9PYk356wWaDMYw79B17iWtfsSMi

Exit codes: 0 walk recorded | 1 usage/input | 2 runtime error.
"""
import argparse
import sys

from . import qdb, xrpl_client
from .config import SEED_TOKENS, SeedToken, WalkerConfig, find_seed, meme_targets
from .walker import STATUS_ERROR, STATUS_SKIPPED, WalkResult, walk_token


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


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="walker",
                                description="Lodestone forensic ledger walker (Phase 2)")
    p.add_argument("--list", action="store_true", help="print the seed registry and exit")
    p.add_argument("--token", help="seed display name (e.g. LEDGERGUY)")
    p.add_argument("--currency", help="raw currency code (3-char or 40-hex)")
    p.add_argument("--issuer", help="issuer classic address (r...)")
    p.add_argument("--all", action="store_true", help="walk every meme seed sequentially")
    p.add_argument("--dry-run", action="store_true", help="no QuestDB writes")
    p.add_argument("--force", action="store_true",
                   help="re-walk past a COMPLETE row (tx_hash dedupe)")
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