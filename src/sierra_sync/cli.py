from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import __version__
from .config.loader import load_config


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
        print("env ok")
        print(f"config.data_root = {cfg.data_root}")
        print(f"config.logs_root = {cfg.logs_root}")
        print(f"config.timezone  = {cfg.timezone}")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
