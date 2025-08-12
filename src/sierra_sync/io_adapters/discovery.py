from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


@dataclass(frozen=True)
class DiscoveredCandidate:
    stem: str
    scid_file: Path | None
    depth_file: Path
    depth_mtime: float  # for tie-breaking


def _depth_for_day(depth_root: Path, symbol: str, day: date) -> list[Path]:
    depth_root = Path(depth_root)
    return [p for p in depth_root.glob(f"{symbol}*.{day.isoformat()}.depth") if p.is_file()]


def _extract_stem(depth_file: Path, day: date) -> str:
    suffix = f".{day.isoformat()}.depth"
    name = depth_file.name
    if not name.endswith(suffix):
        raise ValueError(f"Depth filename not in expected form: {name}")
    return name[: -len(suffix)]


def _matching_scid(scid_root: Path, stem: str) -> Path | None:
    p = Path(scid_root) / f"{stem}.scid"
    return p if p.exists() else None


def discover_by_depth_multi(
    scid_root: Path,
    depth_root: Path,
    symbol: str,
    day: date,
    search_window_days: int = 0,
) -> list[DiscoveredCandidate]:
    """
    Return ALL matching (stem, scid, depth) candidates for the given symbol/day.
    If none on 'day' and search_window_days > 0, scan +/- N days until any appear.
    """
    scid_root = Path(scid_root)
    depth_root = Path(depth_root)

    candidates: list[Path] = _depth_for_day(depth_root, symbol, day)
    if not candidates and search_window_days > 0:
        found_day: date | None = None
        for delta in range(1, search_window_days + 1):
            for d in (day - timedelta(days=delta), day + timedelta(days=delta)):
                more = _depth_for_day(depth_root, symbol, d)
                if more:
                    candidates = more
                    found_day = d
                    break
            if candidates:
                break
        if found_day:
            day = found_day  # use the day we actually found files on

    results: list[DiscoveredCandidate] = []
    for depth in candidates:
        stem = _extract_stem(depth, day)
        scid = _matching_scid(scid_root, stem)
        results.append(
            DiscoveredCandidate(
                stem=stem,
                scid_file=scid,
                depth_file=depth,
                depth_mtime=depth.stat().st_mtime,
            )
        )
    return results


def choose_best(cands: list[DiscoveredCandidate]) -> DiscoveredCandidate | None:
    """
    Heuristic:
      1) Prefer candidates where SCID exists.
      2) Among those, newest depth mtime wins.
      3) If none have SCID, just take newest depth mtime.
    """
    if not cands:
        return None
    with_scid = [c for c in cands if c.scid_file is not None]
    pool = with_scid if with_scid else cands
    return max(pool, key=lambda c: c.depth_mtime)
