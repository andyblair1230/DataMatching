from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from . import __version__
from .config.loader import load_config
from .utils.logging import get_logger


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

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
