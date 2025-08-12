from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Defaults:
    # Where Sierra stores intraday SCID files
    scid_root: Path = Path(r"C:\SierraChart\Data")
    # Where Sierra stores market depth binaries
    depth_root: Path = Path(r"C:\SierraChart\Data\MarketDepthData")
    # Your project logs/artifacts
    logs_root: Path = Path(r"C:\sierra-logs")
    # Sierra runs in New York time by default
    timezone: str = "America/New_York"


DEFAULTS = Defaults()
