from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str
    exchange: str
    allowed_contract_months: tuple[str, ...]
    stem_template: str
    depth_roll_utc: str  # "HH:MM:SS" (UTC)
    description: str | None = None


@dataclass(frozen=True)
class ReferenceData:
    month_code: Mapping[int, str]  # 1->F, 2->G, ...
    instruments: Mapping[str, InstrumentSpec]  # "ES" -> spec
