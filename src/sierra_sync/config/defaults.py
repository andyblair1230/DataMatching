from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Defaults:
    scid_root: str
    depth_root: str
    logs_root: str
    timezone: str
    refdata_file: str | None
    cme_specs_root: str | None  # NEW


DEFAULTS = Defaults(
    scid_root=r"C:\SierraChart\Data",
    depth_root=r"C:\SierraChart\Data\MarketDepthData",
    logs_root=r"C:\sierra-logs",
    timezone="UTC",
    refdata_file=None,  # Optional YAML with instrument templates
    cme_specs_root=None,  # Optional folder with CSVs (e.g. C:\Users\jabla\cme_specs)
)
