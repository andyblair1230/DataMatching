from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ..config.loader import Config
from ..io_adapters.discovery import choose_best, discover_by_depth_multi
from ..utils.logging import get_logger


@dataclass(frozen=True)
class SyncRequest:
    symbol: str
    day: date
    dry_run: bool = True
    run_id: str | None = None
    prefer_stem: str | None = None  # allow explicit pick


def run_sync(cfg: Config, req: SyncRequest) -> int:
    log = get_logger("sierra_sync.sync", logs_root=cfg.logs_root, run_id=req.run_id)

    # Validate roots
    missing = []
    if not Path(cfg.scid_root).exists():
        missing.append(("scid_root", str(cfg.scid_root)))
    if not Path(cfg.depth_root).exists():
        missing.append(("depth_root", str(cfg.depth_root)))
    if missing:
        for k, p in missing:
            log.error("missing_root", extra={"root": k, "path": p})
        return 2

    # Discover all candidates for that day (or nearby)
    cands = discover_by_depth_multi(
        Path(cfg.scid_root), Path(cfg.depth_root), req.symbol, req.day, search_window_days=7
    )
    if not cands:
        log.error("depth_missing_for_day", extra={"symbol": req.symbol, "day": req.day.isoformat()})
        if req.dry_run:
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
            if req.dry_run:
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
        },
    )

    # Dry-run: print a friendly summary
    if req.dry_run:
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

    # Non-dry-run checks
    if chosen is None:
        return 4
    if chosen.scid_file is None:
        log.error("scid_missing", extra={"expected": f"{chosen.stem}.scid"})
        return 3
    if not Path(chosen.depth_file).exists():
        log.error("depth_missing", extra={"expected": chosen.depth_file.name})
        return 4

    # TODO: call the actual ingestion/matching/export here.
    log.info("sync_done", extra={"stem": chosen.stem})
    return 0
