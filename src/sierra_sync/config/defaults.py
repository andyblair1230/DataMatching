from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Defaults:
    data_root: Path
    logs_root: Path
    timezone: str


DEFAULTS = Defaults(
    data_root=Path(r"C:\data"),  # change to your actual default
    logs_root=Path(r"C:\logs"),  # change to your actual default
    timezone="UTC",
)
