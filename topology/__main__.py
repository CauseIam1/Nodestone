"""
Lodestone topology CLI — Phase 3 ops entrypoint (stdlib only).

    cd /home/causeiam/docker-containers/Lodestone
    python3 -m topology --list            # walked-token inventory (read-only)
    python3 -m topology --dry-run         # constellation preview, zero writes
    python3 -m topology                   # map ALL walked tokens (writes)
    python3 -m topology --token LEDGERGUY [--force]

Phase 3 needs LODESTONE_QDB_* only if overriding defaults (walker env-parity).
Exit codes: 0 mapped | 1 usage/input | 2 runtime error.
"""
import argparse
import sys

from walker.qdb import QuestDbClient, QdbError
from walker.config import WalkerConfig
from .qdbx import TopologyClient
from .mapper import map_all


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="topology",
                                description="Lodestone Phase 3 topology mapper")
    p.add_argument("--list", action="store_true",
                   help="show walked tokens (token_lineage LATEST) and exit")
    p.add_argument("--token", help="single walked token by display name")
    p.add_argument("--dry-run", action="store_true",
                   help="constellation preview — no QuestDB writes")
    p.add_argument("--force", action="store_true",
                   help="re-map clusters already present in topology tables")
    args = p.parse_args(argv)

    cfg = WalkerConfig.from_env()
    try:
        tc = TopologyClient(QuestDbClient(cfg.qdb_exec_url, cfg.qdb_ilp_url,
                                          cfg.qdb_timeout_s,
                                          cfg.qdb_max_retries))
        if args.list:
            rows = tc.latest_lineage()
            if not rows:
                print("[·] no walked tokens yet — run the Phase 2 walker")
                return 0
            print(f"{'currency':<14} {'issuer':<36} {'ghost':<36} "
                  f"{'conf':<6} status")
            for r in rows:
                print("%-14s %-36s %-36s %-6s %s" % (
                    r.get("currency"), r.get("issuer"),
                    r.get("funding_wallet") or "-",
                    "%.2f" % (r.get("creator_confidence") or 0.0),
                    r.get("lineage_status")))
            return 0
        report = map_all(cfg, token_filter=args.token,
                         dry_run=args.dry_run, force=args.force)
        if args.token and not report:
            return 1
        print("")
        print("=== TOPOLOGY SUMMARY ===")
        for r in report:
            print("  " + " | ".join(str(x)[:72] for x in r))
    except QdbError as e:
        print("[x] QuestDB failure: %s" % e)
        return 2
    except ValueError as e:
        print("[x] input: %s" % e)
        return 1
    except Exception as e:  # defensive last resort — the CLI must survive
        print("[x] %s: %s" % (type(e).__name__, e))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())