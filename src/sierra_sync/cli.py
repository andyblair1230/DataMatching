from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo  # NEW

from . import __version__
from .config.loader import load_config
from .io_adapters.cme_specs_loader import columns as table_columns
from .io_adapters.cme_specs_loader import list_tables, load_cme_specs
from .io_adapters.cme_specs_loader import rows as table_rows
from .io_adapters.depth_reader import DepthFile
from .pipeline.sync import SyncRequest, run_sync
from .tools import dump as dump_tools
from .utils.logging import get_logger
from .utils.sc_time import sc_microseconds_to_datetime  # NEW


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sierra-sync", description="Sierra trade/depth matcher")
    sub = p.add_subparsers(dest="cmd", required=False)

    # version
    sub.add_parser("version", help="print version")

    # doctor
    doctor = sub.add_parser("doctor", help="basic environment checks and config")
    doctor.add_argument("--config", type=Path, default=None, help="Optional YAML settings file")

    # sync
    sync = sub.add_parser("sync", help="match / optionally export trade/depth for a symbol and day")
    sync.add_argument("symbol", help="Symbol root, e.g. ES, MES, NQ")
    sync.add_argument("date", type=lambda s: date.fromisoformat(s), help="YYYY-MM-DD")
    sync.add_argument("--config", type=Path, default=None, help="Optional YAML settings file")
    sync.add_argument("--dry-run", action="store_true", help="Plan only; do not write outputs")
    sync.add_argument("--stem", type=str, default=None, help="Force contract stem, e.g. ESU25_FUT_CME")
    sync.add_argument("--export", action="store_true", help="Write -SYNC outputs (depth day + SCID day-slice)")
    sync.add_argument("--out-stem", type=str, default=None, help="Override output stem (default: <stem>-SYNC)")
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
            "(prf=Product Reference, gpr=Globex Product Ref, nrr=Non-Reviewable Range, "
            "spl=Special Price Limits, position_limits=all exchanges)"
        ),
    )
    specs.add_argument("-p", "--product", help="Product code, e.g. ES")
    specs.add_argument("-x", "--exchange", help="Exchange, e.g. CME/CBOT/NYMEX/COMEX")
    specs.add_argument("--show-columns", action="store_true", help="Print normalized column names and exit")
    specs.add_argument("--limit", type=int, default=20, help="Limit number of rows printed (default: 20)")
    specs.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")

    # dump (Markdown snippets from real data)
    dump = sub.add_parser(
        "dump",
        help="Dump windows / snapshots as Markdown tables (depth, scid, both, or trades)",
    )
    dump.add_argument("which", choices=["depth", "scid", "both", "trades"], help="Which view to dump")
    dump.add_argument("symbol", help="Symbol root, e.g. ES, NQ, CL")
    dump.add_argument("date", type=lambda s: date.fromisoformat(s), help="YYYY-MM-DD")
    dump.add_argument("--config", type=Path, default=None, help="YAML settings for roots")

    # depth options (UTC day)
    dump.add_argument(
        "--snapshot-at",
        type=str,
        default=None,
        help="For depth: next snapshot at/after HH:MM:SS[.mmm] (UTC day)",
    )
    dump.add_argument(
        "--start",
        type=str,
        default=None,
        help="Window start HH:MM:SS[.mmm] (UTC) if not using --snapshot-at",
    )
    dump.add_argument(
        "--end",
        type=str,
        default=None,
        help="Window end HH:MM:SS[.mmm] (UTC) if not using --snapshot-at",
    )

    # local-time convenience (treat --date as a LOCAL calendar date)
    dump.add_argument(
        "--start-local",
        type=str,
        default=None,
        help="Local window start HH:MM:SS[.mmm] (uses timezone from config)",
    )
    dump.add_argument(
        "--end-local",
        type=str,
        default=None,
        help="Local window end HH:MM:SS[.mmm] (uses timezone from config)",
    )

    # output path (UTF-8)
    dump.add_argument("--out", type=Path, default=None, help="Write UTF-8 Markdown to this path instead of stdout")

    # audit: confirm first record is CLEAR_BOOK for each *FUT_CME*.depth
    audit = sub.add_parser(
        "audit-depth-head",
        help="Check each *FUT_CME*.depth under depth_root starts with CLEAR_BOOK (cmd=1) and print first timestamp in UTC and ET.",
    )
    audit.add_argument("--config", type=Path, default=None, help="YAML settings for roots")
    audit.add_argument(
        "--glob",
        type=str,
        default="*FUT_CME*.depth",
        help="Filename glob to search under depth_root (default: *FUT_CME*.depth)",
    )

    return p  # keep after audit


def _write_or_print(text: str, out_path: Path | None) -> None:
    if out_path is None:
        print(text)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")


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
                "refdata_file": (str(cfg.refdata_file) if getattr(cfg, "refdata_file", None) else None),
                "cme_specs_root": (str(cfg.cme_specs_root) if getattr(cfg, "cme_specs_root", None) else None),
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
            progress=not bool(args.no_progress),
        )
        return run_sync(cfg, req)

    if args.cmd == "specs":
        cfg = load_config(args.config)
        if not getattr(cfg, "cme_specs_root", None):
            print("cme_specs_root is not set; add it to your config or set SIERRA_CME_SPECS_ROOT.")
            return 1

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

        data = table_rows(specs, args.table, product_code=args.product, exchange=args.exchange)
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
            for row in data:
                preview = {k: row.get(k) for k in cols[:8]}
                print(preview)
        return 0

    if args.cmd == "audit-depth-head":
        cfg = load_config(args.config)
        root = Path(cfg.depth_root)
        # match glob and skip any -SYNC files (case-insensitive)
        files = sorted(p for p in root.rglob(args.glob) if "-SYNC" not in p.name.upper())
        if not files:
            print(f"No files matched under {root} with pattern {args.glob} (excluding -SYNC).")
            return 1

        tz_et = ZoneInfo("America/New_York")

        def _fmt_ms(dt: datetime) -> str:
            return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"

        ok = fail = empty = err = 0
        for p in files:
            try:
                with DepthFile(p) as r:
                    it = r.iter_records()
                    first = next(it, None)
                    if first is None:
                        print(f"[EMPTY] {p.name}")
                        empty += 1
                        continue

                    dt_utc = sc_microseconds_to_datetime(first.dt_us).astimezone(ZoneInfo("UTC"))
                    dt_et = dt_utc.astimezone(tz_et)
                    dow = dt_et.strftime("%a")  # day-of-week in ET

                    if first.command == 1:
                        print(f"[OK]   {p.name}  first={_fmt_ms(dt_utc)} UTC | {_fmt_ms(dt_et)} ET ({dow})")
                        ok += 1
                    else:
                        print(f"[FAIL] {p.name}  first={_fmt_ms(dt_utc)} UTC | {_fmt_ms(dt_et)} ET ({dow})  first.cmd={first.command}")
                        fail += 1
            except Exception as e:
                print(f"[ERROR] {p.name}  ({e})")
                err += 1

        total = ok + fail + empty + err
        print(f"\nScanned {total} files under {root} (excluding -SYNC)")
        print(f"OK={ok}  FAIL={fail}  EMPTY={empty}  ERRORS={err}")
        return 0

    # ------------------------------ dump handler
    if args.cmd == "dump":
        cfg = load_config(args.config)
        day_dt_utc = datetime(args.date.year, args.date.month, args.date.day, tzinfo=UTC)

        scid_path, depth_path, stem = dump_tools.resolve_files(Path(cfg.scid_root), Path(cfg.depth_root), args.symbol, day_dt_utc)
        if not stem:
            print("No candidates discovered for that symbol/day.")
            return 2

        # Determine time window (UTC microseconds)
        if getattr(args, "start_local", None) and getattr(args, "end_local", None):
            start_us, end_us = dump_tools.window_us_for_local_day(args.date, cfg.timezone, args.start_local, args.end_local)
            header_window = f"{args.start_local}–{args.end_local} {cfg.timezone}"
        elif getattr(args, "start", None) and getattr(args, "end", None):
            start_us, end_us = dump_tools.window_us_for_day(day_dt_utc, args.start, args.end)
            header_window = f"{args.start}–{args.end} UTC"
        elif getattr(args, "snapshot_at", None):
            start_us = end_us = -1  # snapshot path below
            header_window = f"snapshot at/after {args.snapshot_at} (UTC day)"
        else:
            print("Provide --snapshot-at OR (--start & --end) OR (--start-local & --end-local).")
            return 1

        if args.which == "depth":
            if depth_path is None or not depth_path.exists():
                print("Depth file not found.")
                return 3

            if args.snapshot_at:
                at_us = dump_tools.snapshot_start_us_for_day(day_dt_utc, args.snapshot_at)
                batch = dump_tools._depth_snapshot_at_or_after(depth_path, at_us)
                text = "\n".join(
                    [
                        f"# Depth snapshot for {stem} at/after {args.snapshot_at} UTC",
                        "",
                        dump_tools.depth_to_markdown(batch) if batch else "_none_",
                        "",
                    ]
                )
                _write_or_print(text, args.out)
                return 0

            depth_records = dump_tools._depth_records_in_window(depth_path, start_us, end_us)
            text = "\n".join(
                [
                    f"# Depth window for {stem} [{header_window}]",
                    "",
                    dump_tools.depth_to_markdown(depth_records) if depth_records else "_none_",
                    "",
                ]
            )
            _write_or_print(text, args.out)
            return 0

        if args.which == "scid":
            if scid_path is None or not scid_path.exists():
                print("SCID file not found.")
                return 3
            if args.snapshot_at:
                print("For SCID dump, use --start/--end or --start-local/--end-local.")
                return 1
            scid_records = dump_tools._scid_records_in_window(scid_path, start_us, end_us)
            text = "\n".join(
                [
                    f"# SCID window for {stem} [{header_window}]",
                    "",
                    dump_tools.scid_to_markdown(scid_records) if scid_records else "_none_",
                    "",
                ]
            )
            _write_or_print(text, args.out)
            return 0

        if args.which == "both":
            if depth_path is None or not depth_path.exists():
                print("Depth file not found.")
                return 3
            if args.snapshot_at:
                print("Use 'both' with --start/--end or --start-local/--end-local.")
                return 1

            depth_records = dump_tools._depth_records_in_window(depth_path, start_us, end_us)
            scid_records = dump_tools._scid_records_in_window(scid_path, start_us, end_us) if scid_path and scid_path.exists() else []
            # side-by-side, zipped by millisecond
            body = dump_tools.both_window_markdown_side_by_side(depth_records, scid_records)
            text = "\n".join(
                [
                    f"# Depth + SCID (side-by-side) for {stem} [{header_window}]",
                    "",
                    body,
                    "",
                ]
            )
            _write_or_print(text, args.out)
            return 0

        if args.which == "trades":
            if depth_path is None or not depth_path.exists():
                print("Depth file not found.")
                return 3
            if scid_path is None or not scid_path.exists():
                print("SCID file not found.")
                return 3
            if args.snapshot_at:
                print("Use 'trades' with --start/--end or --start-local/--end-local.")
                return 1

            scid_rows = dump_tools._scid_records_in_window(scid_path, start_us, end_us)
            depth_rows = dump_tools._depth_records_in_window(depth_path, start_us, end_us)

            # side-by-side per millisecond (Depth left, SCID right)
            body = dump_tools.trades_window_markdown_side_by_side(scid_rows, depth_rows)

            text = "\n".join(
                [
                    f"# Trades window for {stem} [{header_window}]",
                    "",
                    body,
                    "",
                ]
            )
            _write_or_print(text, args.out)
            return 0

    # ---------------------------------------------------------------

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
