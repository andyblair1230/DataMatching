from __future__ import annotations

from pathlib import Path

import yaml

from sierra_sync.model.refdata import InstrumentSpec, ReferenceData


def load_refdata(path: Path) -> ReferenceData:
    """
    Load instruments.yaml into typed ReferenceData.
    """
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Invalid refdata YAML: expected a mapping")

    month_code_raw = data.get("month_code") or {}
    if not isinstance(month_code_raw, dict):
        raise ValueError("refdata.month_code must be a mapping")

    instr_raw = data.get("instruments") or {}
    if not isinstance(instr_raw, dict):
        raise ValueError("refdata.instruments must be a mapping")

    instruments: dict[str, InstrumentSpec] = {}
    for sym, row in instr_raw.items():
        if not isinstance(row, dict):
            raise ValueError(f"instruments.{sym} must be a mapping")

        allowed = tuple(row.get("allowed_contract_months") or ())
        spec = InstrumentSpec(
            symbol=sym,
            exchange=str(row.get("exchange") or ""),
            allowed_contract_months=allowed,
            stem_template=str(row.get("stem_template") or ""),
            depth_roll_utc=str(row.get("depth_roll_utc") or "00:00:00"),
            description=(str(row["description"]) if "description" in row else None),
        )
        instruments[sym] = spec

    return ReferenceData(month_code=month_code_raw, instruments=instruments)
