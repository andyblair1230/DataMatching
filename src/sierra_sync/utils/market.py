from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# CME month codes (standard for your data)
MONTH_CODE = {
    1: "F",  # Jan
    2: "G",  # Feb
    3: "H",  # Mar
    4: "J",  # Apr
    5: "K",  # May
    6: "M",  # Jun
    7: "N",  # Jul
    8: "Q",  # Aug
    9: "U",  # Sep
    10: "V",  # Oct
    11: "X",  # Nov
    12: "Z",  # Dec
}


@dataclass(frozen=True)
class ContractId:
    """Resolved contract identifier parts for filename stems."""

    symbol: str  # e.g., "ES"
    month_code: str  # e.g., "U"
    yy: str  # e.g., "25"
    suffix: str  # e.g., "_FUT_CME"

    def stem(self) -> str:
        """Build the filename stem used by SCID/depth."""
        return f"{self.symbol}{self.month_code}{self.yy}{self.suffix}"


def month_code_for(d: date) -> str:
    """Map a calendar month to the futures month letter."""
    try:
        return MONTH_CODE[d.month]
    except KeyError:
        raise ValueError(f"Unsupported month: {d.month}") from None


def two_digit_year(d: date) -> str:
    """Return YY (00-99)."""
    return f"{d.year % 100:02d}"


def infer_suffix_from_existing(scid_root: Path, symbol: str) -> str | None:
    """
    Look at existing SCID files starting with the symbol and infer
    the most common suffix pattern.
    For your setup, this will almost always be "_FUT_CME".
    """
    scid_root = Path(scid_root)
    candidates = list(scid_root.glob(f"{symbol}*.scid"))
    if not candidates:
        return None

    suffixes: list[str] = []
    for p in candidates:
        stem = p.stem  # no extension
        # Month code (1 char) + year (2 chars) => 3 chars after symbol
        base = stem[len(symbol) :]
        if len(base) >= 3:
            suffix = base[3:]
            if suffix:
                suffixes.append(suffix)
    if not suffixes:
        return None

    return Counter(suffixes).most_common(1)[0][0]


def build_contract_id(scid_root: Path, symbol: str, d: date) -> ContractId:
    """Build a ContractId using the month code, year, and inferred suffix."""
    mcode = month_code_for(d)
    yy = two_digit_year(d)
    suffix = infer_suffix_from_existing(scid_root, symbol) or "_FUT_CME"
    return ContractId(symbol=symbol, month_code=mcode, yy=yy, suffix=suffix)


def candidate_depth_filename(depth_root: Path, contract: ContractId, day: date) -> Path:
    """Depth filenames are: <STEM>.<YYYY-MM-DD>.depth."""
    return Path(depth_root) / f"{contract.stem()}.{day.isoformat()}.depth"


def matching_scid_file(scid_root: Path, contract: ContractId) -> Path | None:
    """Find the SCID file that matches the stem exactly."""
    p = Path(scid_root) / f"{contract.stem()}.scid"
    return p if p.exists() else None


def list_scids_for_symbol(scid_root: Path, symbol: str) -> Iterable[Path]:
    """List all SCID files for a symbol."""
    yield from Path(scid_root).glob(f"{symbol}*.scid")
