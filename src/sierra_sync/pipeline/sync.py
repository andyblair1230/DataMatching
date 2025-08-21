from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ..config.loader import Config
from ..io_adapters.depth_reader import DepthFile
from ..io_adapters.depth_writer import DepthWriter, DepthWriteRecord
from ..io_adapters.discovery import choose_best, discover_by_depth_multi
from ..io_adapters.scid_reader import ScidFile
from ..io_adapters.scid_writer import ScidWriter, ScidWriteRecord
from ..utils.logging import get_logger
from ..utils.sc_time import datetime_to_sc_microseconds


@dataclass(frozen=True)
class SyncRequest:
    symbol: str
    day: date
    dry_run: bool = True
    run_id: str | None = None
    prefer_stem: str | None = None  # allow explicit pick

    # Passthrough export options
    export: bool = False
    out_stem: str | None = None  # default: <chosen.stem>-SYNC

    # Progress meter
    progress: bool = True


# --- helpers -----------------------------------------------------------------


def _utc_bounds_for_day(d: date) -> tuple[int, int]:
    """Return [start_us, end_us) SC microseconds for that UTC calendar day."""
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    return (
        datetime_to_sc_microseconds(start),
        datetime_to_sc_microseconds(end),
    )


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    units = ["KB", "MB", "GB", "TB"]
    val = float(n)
    for u in units:
        val /= 1024.0
        if val < 1024.0:
            return f"{val:,.2f} {u}"
    return f"{val:,.2f} PB"


def _progress_line(
    label: str,
    done: int,
    total: int,
    start_ts: float,
    bar_width: int = 40,
    unit: str = "rec",
) -> str:
    """
    Build a single-line progress meter like:
      Depth export  [=======>.....]  63.0%  elapsed  12.3s  1.2M rec/s
    """
    now = time.perf_counter()
    elapsed = max(now - start_ts, 1e-9)

    pct = 1.0 if total <= 0 else min(max(done / total, 0.0), 1.0)
    fills = int(round(pct * bar_width))
    bar = "[" + "=" * max(fills - 1, 0) + (">" if 0 < pct < 1 else "=" if pct == 1 else "") + "." * (bar_width - fills) + "]"

    rate = done / elapsed
    if unit == "bytes":
        rate_str = f"{_fmt_bytes(int(rate))}/s"
    else:
        if rate >= 1_000_000:
            rate_str = f"{rate/1_000_000:.2f}M rec/s"
        elif rate >= 1_000:
            rate_str = f"{rate/1_000:.2f}K rec/s"
        else:
            rate_str = f"{rate:.0f} rec/s"

    pct_str = f"{pct*100:5.1f}%"
    return f"{label:<12} {bar}  {pct_str}  elapsed {elapsed:6.1f}s  {rate_str}"


def _print_progress(
    label: str,
    done: int,
    total: int,
    start_ts: float,
    unit: str = "rec",
    final: bool = False,
) -> None:
    line = _progress_line(label, done, total, start_ts, unit=unit)
    end = "\n" if final else "\r"
    print(line, end=end, flush=True)


# --- main pipeline -----------------------------------------------------------


def run_sync(cfg: Config, req: SyncRequest) -> int:
    log = get_logger("sierra_sync.sync", logs_root=cfg.logs_root, run_id=req.run_id)

    # Validate roots
    missing: list[tuple[str, str]] = []
    if not Path(cfg.scid_root).exists():
        missing.append(("scid_root", str(cfg.scid_root)))
    if not Path(cfg.depth_root).exists():
        missing.append(("depth_root", str(cfg.depth_root)))
    if missing:
        for k, p in missing:
            log.error("missing_root", extra={"root": k, "path": p})
        return 2

    # Discover all candidates for that day (or nearby)
    cands = discover_by_depth_multi(Path(cfg.scid_root), Path(cfg.depth_root), req.symbol, req.day, search_window_days=7)
    if not cands:
        log.error(
            "depth_missing_for_day",
            extra={"symbol": req.symbol, "day": req.day.isoformat()},
        )
        if req.dry_run and not req.export:
            print(f"No depth files found for {req.symbol} on/near {req.day.isoformat()}")
        return 4

    # Pick specific stem if requested
    chosen = None
    if req.prefer_stem:
        for c in cands:
            if c.stem.lower() == req.prefer_stem.lower():
                chosen = c
                break
        if not chosen:
            log.error("preferred_stem_not_found", extra={"prefer_stem": req.prefer_stem})
            if req.dry_run and not req.export:
                print(f"Requested stem {req.prefer_stem} not found among candidates.")
            return 4
    else:
        chosen = choose_best(cands)

    # Log plan
    log.info(
        "sync_plan_multi",
        extra={
            "symbol": req.symbol,
            "day": req.day.isoformat(),
            "candidates": [
                {
                    "stem": c.stem,
                    "scid_path": str(c.scid_file) if c.scid_file else None,
                    "depth_path": str(c.depth_file),
                    "depth_exists": Path(c.depth_file).exists(),
                    "depth_mtime": c.depth_mtime,
                }
                for c in cands
            ],
            "chosen": {
                "stem": chosen.stem if chosen else None,
                "scid_path": str(chosen.scid_file) if (chosen and chosen.scid_file) else None,
                "depth_path": str(chosen.depth_file) if chosen else None,
            },
            "dry_run": req.dry_run,
            "export": req.export,
        },
    )

    # Dry-run (no export): print a friendly summary
    if req.dry_run and not req.export:
        print("Candidates:")
        for c in sorted(cands, key=lambda x: x.depth_mtime, reverse=True):
            sc = str(c.scid_file) if c.scid_file else "SCID: MISSING"
            ex = "(exists)" if Path(c.depth_file).exists() else "(missing)"
            print(f"  - {c.stem} | {c.depth_file.name} {ex} | {sc}")
        if chosen:
            print("\nChosen:")
            print(f"  Contract ID: {chosen.stem}")
            print(f"  SCID file:   {chosen.scid_file if chosen.scid_file else 'Not found'}")
            print(f"  Depth file:  {chosen.depth_file} (exists)")
        return 0

    # Non-dry-run checks for export/matching
    if chosen is None:
        return 4
    if chosen.scid_file is None:
        log.error("scid_missing", extra={"expected": f"{chosen.stem}.scid"})
        return 3
    if not Path(chosen.depth_file).exists():
        log.error("depth_missing", extra={"expected": chosen.depth_file.name})
        return 4

    # --- Passthrough export (safe write) ------------------------------------
    if req.export:
        # If user provides --out-stem, use it verbatim; else append "-SYNC" once.
        if req.out_stem:
            out_stem = req.out_stem
        else:
            out_stem = chosen.stem
            if not out_stem.endswith("-SYNC"):
                out_stem += "-SYNC"

        overall_t0 = time.perf_counter()

        # 1) Depth passthrough: copy ALL records for the day
        depth_out = cfg.depth_root / f"{out_stem}.{req.day.isoformat()}.depth"
        depth_start = time.perf_counter()
        depth_rows = 0
        with DepthFile(Path(chosen.depth_file)) as r, DepthWriter(depth_out) as w:
            total = len(r)
            last_tick = depth_start
            for drec in r.iter_records():
                w.append(
                    DepthWriteRecord(
                        dt_us=drec.dt_us,
                        command=drec.command,
                        flags=drec.flags,
                        num_orders=drec.num_orders,
                        price=drec.price,
                        quantity=drec.quantity,
                        reserved=drec.reserved,
                    )
                )
                depth_rows += 1
                if req.progress:
                    now = time.perf_counter()
                    if now - last_tick >= 0.05:
                        _print_progress("Depth export", depth_rows, total, depth_start, unit="rec", final=False)
                        last_tick = now
            if req.progress:
                _print_progress("Depth export", depth_rows, total, depth_start, unit="rec", final=True)
        log.info("export_depth_done", extra={"out": str(depth_out)})

        # 2) SCID passthrough: slice to [00:00, 24:00) UTC of req.day
        start_us, end_us = _utc_bounds_for_day(req.day)
        scid_out = cfg.scid_root / f"{out_stem}.scid"

        # Pre-scan total rows in-window (for a correct 100% progress)
        total_in_window = 0
        with ScidFile(Path(chosen.scid_file)) as r:
            for srec in r.iter_records():
                if start_us <= srec.dt_us < end_us:
                    total_in_window += 1

        scid_start = time.perf_counter()
        scid_rows = 0
        with ScidFile(Path(chosen.scid_file)) as r, ScidWriter(scid_out) as w:
            last_tick = scid_start
            for srec in r.iter_records():
                if start_us <= srec.dt_us < end_us:
                    w.append(
                        ScidWriteRecord(
                            dt_us=srec.dt_us,
                            open=srec.open,
                            high=srec.high,
                            low=srec.low,
                            close=srec.close,
                            num_trades=srec.num_trades,
                            total_volume=srec.total_volume,
                            bid_volume=srec.bid_volume,
                            ask_volume=srec.ask_volume,
                        )
                    )
                    scid_rows += 1
                    if req.progress:
                        now = time.perf_counter()
                        if now - last_tick >= 0.05:
                            _print_progress(
                                "SCID export",
                                scid_rows,
                                total_in_window,
                                scid_start,
                                unit="rec",
                                final=False,
                            )
                            last_tick = now
            if req.progress:
                _print_progress("SCID export", scid_rows, total_in_window, scid_start, unit="rec", final=True)
        log.info("export_scid_done", extra={"out": str(scid_out), "rows": scid_rows})

        # Summary
        depth_size = os.path.getsize(depth_out) if Path(depth_out).exists() else 0
        scid_size = os.path.getsize(scid_out) if Path(scid_out).exists() else 0
        total_bytes = depth_size + scid_size

        overall_elapsed = max(time.perf_counter() - overall_t0, 1e-9)
        mb_per_s = (total_bytes / (1024 * 1024)) / overall_elapsed
        recs_total = depth_rows + scid_rows
        recs_per_s = recs_total / overall_elapsed

        print("Export complete:")
        print(f"  Depth  -> {depth_out} (rows: {depth_rows:,}, size: {_fmt_bytes(depth_size)})")
        print(f"  SCID   -> {scid_out} (rows: {scid_rows:,}, size: {_fmt_bytes(scid_size)})")
        print(f"  Total  -> {_fmt_bytes(total_bytes)} in {overall_elapsed:0.2f}s" f"  |  ~{mb_per_s:0.2f} MB/s  |  ~{recs_per_s:,.0f} rec/s")
        return 0

    # Placeholder for future matcher
    log.info("sync_done_no_export")
    return 0
