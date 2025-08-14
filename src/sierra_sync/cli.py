from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import cast

from . import __version__
from .config.loader import load_config
from .io_adapters.cme_specs_loader import columns as table_columns
from .io_adapters.cme_specs_loader import list_tables, load_cme_specs
from .io_adapters.cme_specs_loader import rows as table_rows
from .pipeline.sync import SyncRequest, run_sync
from .utils.logging import get_logger


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sierra-sync", description="Sierra trade/depth matcher")
    sub = p.add_subparsers(dest="cmd", required=False)

    # version
    sub.add_parser("version", help="print version")

    # doctor
    doctor = sub.add_parser("doctor", help="basic environment checks and config")
    doctor.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional path to a YAML settings file to load",
    )

    # sync
    sync = sub.add_parser("sync", help="match / optionally export trade/depth for a symbol and day")
    sync.add_argument("symbol", help="Symbol root, e.g. ES, MES, NQ")
    sync.add_argument("date", type=lambda s: date.fromisoformat(s), help="YYYY-MM-DD")
    sync.add_argument("--config", type=Path, default=None, help="Optional YAML settings file")
    sync.add_argument("--dry-run", action="store_true", help="Plan only; do not write outputs")
    sync.add_argument(
        "--stem",
        type=str,
        default=None,
        help="Force contract stem, e.g. ESU25_FUT_CME",
    )
    sync.add_argument(
        "--export",
        action="store_true",
        help="Write -SYNC outputs (depth day + SCID day-slice)",
    )
    sync.add_argument(
        "--out-stem",
        type=str,
        default=None,
        help="Override output stem (default: <stem>-SYNC)",
    )
    sync.add_argument("--no-progress", action="store_true", help="Disable live progress meter")

    # specs
    specs = sub.add_parser("specs", help="inspect CME specs CSVs (ref data)")
    specs.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional YAML settings file (to pick up cme_specs_root)",
    )
    specs.add_argument(
        "--table",
        choices=["prf", "gpr", "nrr", "spl", "position_limits"],
        help=(
            "Which table to query "
            "(prf=Product Reference, "
            "gpr=Globex Product Ref, "
            "nrr=Non-Reviewable Range, "
            "spl=Special Price Limits, "
            "position_limits=all exchanges)"
        ),
    )
    specs.add_argument("-p", "--product", help="Product code, e.g. ES")
    specs.add_argument("-x", "--exchange", help="Exchange, e.g. CME/CBOT/NYMEX/COMEX")
    specs.add_argument(
        "--show-columns",
        action="store_true",
        help="Print normalized column names for the selected table and exit",
    )
    specs.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit number of rows printed (default: 20)",
    )
    specs.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Output JSON instead of pretty text",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "version":
        print(__version__)
        return 0

    if args.cmd == "doctor":
        cfg = load_config(args.config)

        # per-run log file under logs_root/YYYYMMDD/HHMMSS.log
        run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        log = get_logger("sierra_sync.doctor", logs_root=cfg.logs_root, run_id=run_id)

        print("env ok")
        print(f"config.scid_root      = {cfg.scid_root}")
        print(f"config.depth_root     = {cfg.depth_root}")
        print(f"config.logs_root      = {cfg.logs_root}")
        print(f"config.timezone       = {cfg.timezone}")
        if getattr(cfg, "refdata_file", None):
            print(f"config.refdata_file   = {cfg.refdata_file}")
        if getattr(cfg, "cme_specs_root", None):
            print(f"config.cme_specs_root = {cfg.cme_specs_root}")

        log.info(
            "doctor_config",
            extra={
                "scid_root": str(cfg.scid_root),
                "depth_root": str(cfg.depth_root),
                "logs_root": str(cfg.logs_root),
                "timezone": cfg.timezone,
                "refdata_file": (
                    str(cfg.refdata_file) if getattr(cfg, "refdata_file", None) else None
                ),
                "cme_specs_root": (
                    str(cfg.cme_specs_root) if getattr(cfg, "cme_specs_root", None) else None
                ),
                "run_id": run_id,
            },
        )
        return 0

    if args.cmd == "sync":
        cfg = load_config(args.config)
        run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        req = SyncRequest(
            symbol=args.symbol,
            day=args.date,
            dry_run=bool(args.dry_run),
            run_id=run_id,
            prefer_stem=args.stem,
            export=bool(getattr(args, "export", False)),
            out_stem=getattr(args, "out_stem", None),
            progress=not bool(getattr(args, "no_progress", False)),
        )
        return run_sync(cfg, req)

    if args.cmd == "specs":
        cfg = load_config(args.config)
        if not getattr(cfg, "cme_specs_root", None):
            print("cme_specs_root is not set; add it to your config or set SIERRA_CME_SPECS_ROOT.")
            return 1

        # mypy: after the guard above, this is non-None
        specs = load_cme_specs(cast(Path, cfg.cme_specs_root))

        # no table -> list and exit
        if not getattr(args, "table", None):
            print("Available tables:", ", ".join(list_tables(specs)))
            print("Use: python -m sierra_sync specs --table prf -x CME -p ES")
            return 0

        if args.show_columns:
            cols = table_columns(specs, args.table)
            print(f"{args.table} columns ({len(cols)}):")
            for c in cols:
                print(f"  - {c}")
            return 0

        data = table_rows(
            specs,
            args.table,
            product_code=args.product,
            exchange=args.exchange,
        )
        data = data[: max(0, int(args.limit))] if args.limit else data

        if args.as_json:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            if not data:
                print("No rows.")
                return 0
            cols = table_columns(specs, args.table)
            head = ", ".join(cols[:8])
            print(f"{args.table} rows (showing up to {args.limit}):")
            print(f"Columns (first 8): {head}")
            print("-" * 60)
            for r in data:
                preview = {k: r.get(k) for k in cols[:8]}
                print(preview)
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
