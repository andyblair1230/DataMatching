from __future__ import annotations

import textwrap
from pathlib import Path

from sierra_sync.io_adapters.refdata_loader import load_refdata


def test_load_refdata(tmp_path: Path) -> None:
    yml = tmp_path / "instruments.yaml"
    yml.write_text(
        textwrap.dedent(
            """
            version: 1
            month_code: {1: F, 3: H, 9: U, 12: Z}
            instruments:
              ES:
                description: E-mini S&P 500
                exchange: CME
                allowed_contract_months: [H, M, U, Z]
                stem_template: "{SYMBOL}{MONTH}{YY}_FUT_CME"
                depth_roll_utc: "00:00:00"
            """
        ).strip(),
        encoding="utf-8",
    )

    rd = load_refdata(yml)
    assert rd.month_code[1] == "F"
    assert "ES" in rd.instruments
    es = rd.instruments["ES"]
    assert es.symbol == "ES"
    assert es.exchange == "CME"
    assert set(es.allowed_contract_months) == {"H", "M", "U", "Z"}
    assert es.stem_template.endswith("_FUT_CME")
