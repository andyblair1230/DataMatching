from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ..config.loader import Config
from ..utils.logging import get_logger


@dataclass(frozen=True)
class SyncRequest:
    symbol: str
    day: date
    # later: session boundaries, contract month, etc.
    dry_run: bool = True
    run_id: str | None = None


def run_sync(cfg: Config, req: SyncRequest) -> int:
    """
    Skeleton sync runner: validates roots, logs intent, returns 0 on success.
    Real file discovery/matching will be added next.
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
        return 2  # config error

    # For now, just log what we WOULD do.
    log.info(
        "sync_start",
        extra={
            "symbol": req.symbol,
            "day": req.day.isoformat(),
            "scid_root": str(cfg.scid_root),
            "depth_root": str(cfg.depth_root),
            "dry_run": req.dry_run,
            "run_id": req.run_id,
        },
    )

    # TODO: discover SCID/depth files for (symbol, day), then call matcher pipeline.
    # TODO: read, align, write outputs back to SCID/MarketDepthData.

    log.info("sync_done", extra={"symbol": req.symbol, "day": req.day.isoformat()})
    return 0
