from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

from . import __version__
from .config.loader import load_config
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
    sync = sub.add_parser("sync", help="match trade/depth for a symbol and day")
    sync.add_argument("symbol", help="Symbol root, e.g. ES, MES, NQ")
    sync.add_argument("date", type=lambda s: date.fromisoformat(s), help="YYYY-MM-DD")
    sync.add_argument("--config", type=Path, default=None, help="Optional YAML settings file")
    sync.add_argument("--dry-run", action="store_true", help="Plan only; do not write outputs")
    sync.add_argument(
        "--stem", type=str, default=None, help="Force contract stem, e.g. ESU25_FUT_CME"
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
        )
        return run_sync(cfg, req)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
