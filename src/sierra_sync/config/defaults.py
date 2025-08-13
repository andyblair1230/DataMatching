from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Defaults:
    scid_root: str = r"C:\SierraChart\Data"
    depth_root: str = r"C:\SierraChart\Data\MarketDepthData"
    logs_root: str = r"C:\sierra-logs"
    timezone: str = "America/New_York"
    refdata_file: str = r"C:\dev\DataMatching\refdata\instruments.yaml"  # NEW


DEFAULTS = Defaults()
