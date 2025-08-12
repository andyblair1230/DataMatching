from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ..config.loader import Config
from ..io_adapters.discovery import discover_by_depth
from ..utils.logging import get_logger


@dataclass(frozen=True)
class SyncRequest:
    symbol: str
    day: date
    dry_run: bool = True
    run_id: str | None = None


def run_sync(cfg: Config, req: SyncRequest) -> int:
    """
    Validate roots, locate SCID/depth by inspecting depth filenames for the date.
    Return codes:
      0 = ok (dry-run or actual run)
      2 = config/root error
      3 = scid not found for detected stem
      4 = depth not found (even after fallback window)
    """
    log = get_logger("sierra_sync.sync", logs_root=cfg.logs_root, run_id=req.run_id)

    # Validate roots exist
    missing = []
    if not Path(cfg.scid_root).exists():
        missing.append(("scid_root", str(cfg.scid_root)))
    if not Path(cfg.depth_root).exists():
        missing.append(("depth_root", str(cfg.depth_root)))
    if missing:
        for k, p in missing:
            log.error("missing_root", extra={"root": k, "path": p})
        return 2

    # Discover by depth filename (authoritative). Search +/- 7 days if needed.
    found = discover_by_depth(
        Path(cfg.scid_root), Path(cfg.depth_root), req.symbol, req.day, search_window_days=7
    )
    if not found:
        log.error("depth_missing_for_day", extra={"symbol": req.symbol, "day": req.day.isoformat()})
        return 4

    scid_str = str(found.scid_file) if found.scid_file else None
    depth_str = str(found.depth_file) if found.depth_file else None

    log.info(
        "sync_plan",
        extra={
            "symbol": req.symbol,
            "day": req.day.isoformat(),
            "stem": found.stem,
            "scid_path": scid_str,
            "depth_path": depth_str,
            "depth_exists": bool(found.depth_file and Path(found.depth_file).exists()),
            "dry_run": req.dry_run,
        },
    )

    if found.scid_file is None:
        log.error("scid_missing", extra={"expected": f"{found.stem}.scid"})
        # In dry-run, also show the expected path for clarity
        if req.dry_run:
            print("SCID file:   Not found (expected:", f"{found.stem}.scid", ")")
            if depth_str:
                print(
                    "Depth file:  ",
                    depth_str,
                    "(exists)" if Path(depth_str).exists() else "(missing)",
                )
        return 3

    if not (found.depth_file and Path(found.depth_file).exists()):
        log.error("depth_missing", extra={"expected": f"{found.stem}.{req.day.isoformat()}.depth"})
        if req.dry_run:
            print(f"Contract ID: {found.stem}")
            print(f"SCID file:   {scid_str}")
            if depth_str:
                print("Depth file:  ", depth_str, "(missing)")
        return 4

    # Dry-run: print the resolution so the user sees exactly what would be used
    if req.dry_run:
        print(f"Contract ID: {found.stem}")
        print(f"SCID file:   {scid_str}")
        print(f"Depth file:  {depth_str} (exists)")
        log.info("sync_dry_run_done", extra={"stem": found.stem})
        return 0

    # TODO: real ingestion/matching/export goes here for non-dry-run mode.
    log.info("sync_done", extra={"stem": found.stem})
    return 0
