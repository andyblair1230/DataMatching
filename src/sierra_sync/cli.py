from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from . import __version__
from .config.loader import load_config
from .utils.logging import get_logger
from .utils.market import build_contract_id, candidate_depth_filename, matching_scid_file


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sierra-sync", description="Sierra trade/depth matcher")
    sub = p.add_subparsers(dest="cmd", required=False)

    sub.add_parser("version", help="print version")

    doctor = sub.add_parser("doctor", help="basic environment checks and config")
    doctor.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional path to a YAML settings file to load",
    )

    sync = sub.add_parser("sync", help="find matching SCID and depth files for a given symbol/date")
    sync.add_argument("symbol", help="Symbol root, e.g. ES or MES")
    sync.add_argument("date", help="Trading date in YYYY-MM-DD format")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "version":
        print(__version__)
        return 0

    if args.cmd == "doctor":
        cfg = load_config(args.config)
        run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        log = get_logger("sierra_sync.doctor", logs_root=cfg.logs_root, run_id=run_id)

        print("env ok")
        print(f"config.scid_root  = {cfg.scid_root}")
        print(f"config.depth_root = {cfg.depth_root}")
        print(f"config.logs_root  = {cfg.logs_root}")
        print(f"config.timezone   = {cfg.timezone}")

        log.info(
            "doctor_config",
            extra={
                "scid_root": str(cfg.scid_root),
                "depth_root": str(cfg.depth_root),
                "logs_root": str(cfg.logs_root),
                "timezone": cfg.timezone,
                "run_id": run_id,
            },
        )
        return 0

    if args.cmd == "sync":
        cfg = load_config(None)
        try:
            day = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD.")
            return 1

        contract = build_contract_id(cfg.scid_root, args.symbol, day)
        scid_path = matching_scid_file(cfg.scid_root, contract)
        depth_path = candidate_depth_filename(cfg.depth_root, contract, day)

        print(f"Contract ID: {contract.stem()}")
        print(f"SCID file:   {scid_path if scid_path else 'Not found'}")
        print(f"Depth file:  {depth_path if depth_path.exists() else 'Not found'}")

        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
