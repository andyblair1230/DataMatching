from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


@dataclass(frozen=True)
class DiscoveredPaths:
    stem: str
    scid_file: Path | None
    depth_file: Path | None


def _depth_for_day(depth_root: Path, symbol: str, day: date) -> list[Path]:
    """Return all depth files for a symbol on a specific day."""
    depth_root = Path(depth_root)
    return [p for p in depth_root.glob(f"{symbol}*.{day.isoformat()}.depth") if p.is_file()]


def _extract_stem(depth_file: Path, day: date) -> str:
    """From '<STEM>.<YYYY-MM-DD>.depth' recover STEM exactly."""
    suffix = f".{day.isoformat()}.depth"
    name = depth_file.name
    if not name.endswith(suffix):
        raise ValueError(f"Depth filename not in expected form: {name}")
    return name[: -len(suffix)]  # everything before '.YYYY-MM-DD.depth'


def _matching_scid(scid_root: Path, stem: str) -> Path | None:
    p = Path(scid_root) / f"{stem}.scid"
    return p if p.exists() else None


def discover_by_depth(
    scid_root: Path,
    depth_root: Path,
    symbol: str,
    day: date,
    search_window_days: int = 0,
) -> DiscoveredPaths | None:
    """
    Preferred discovery:
      - Look for depth file(s) on 'day' for 'symbol'.
      - If multiple, pick the newest by mtime.
      - Extract <stem> from filename and find '<stem>.scid'.

    If none on 'day' and search_window_days > 0, search +/- N days and
    pick the nearest day with depth; otherwise return None.
    """
    candidates = _depth_for_day(depth_root, symbol, day)
    if not candidates and search_window_days > 0:
        for delta in range(1, search_window_days + 1):
            for d in (day - timedelta(days=delta), day + timedelta(days=delta)):
                more = _depth_for_day(depth_root, symbol, d)
                if more:
                    candidates = more
                    day = d
                    break
            if candidates:
                break

    if not candidates:
        return None

    depth = max(candidates, key=lambda p: p.stat().st_mtime)
    stem = _extract_stem(depth, day)
    scid = _matching_scid(scid_root, stem)
    return DiscoveredPaths(stem=stem, scid_file=scid, depth_file=depth)
