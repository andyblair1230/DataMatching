from __future__ import annotations

import argparse
import sys

from . import __version__


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sierra-sync", description="Sierra trade/depth matcher")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("version", help="print version")
    sub.add_parser("doctor", help="basic environment checks")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "version":
        print(__version__)
        return 0

    if args.cmd == "doctor":
        # keep it dumb for now; we’ll flesh it out later
        print("env ok")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
