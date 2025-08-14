from __future__ import annotations

from pathlib import Path

from sierra_sync.io_adapters.cme_specs_loader import (
    CmeSpecs,
    dollars_per_tick,
    first,
    list_tables,
    load_cme_specs,
    rows,
    tick_size,
)


def _write_csv(p: Path, headers: list[str], rows_: list[list[str]]) -> None:
    p.write_text(",".join(headers) + "\n", encoding="utf-8")
    with p.open("a", encoding="utf-8") as fh:
        for r in rows_:
            fh.write(",".join(r) + "\n")


def test_generic_accessors(tmp_path: Path) -> None:
    # Minimal PRF with ES; add extra columns to prove we keep everything
    _write_csv(
        tmp_path / "cmeg.fut.prf.csv",
        ["Exch", "Sym", "Desc", "Mult", "MinPxIncr", "PriceDisplayFactor", "Some Extra"],
        [["CME", "ES", "E-mini S&P 500", "50", "0.25", "1", "hello"]],
    )

    # Globex product reference with trading params
    _write_csv(
        tmp_path / "globex-product-reference-sheet.csv",
        ["Exchange", "Product Code", "Matching Algorithm", "Price Band Points"],
        [["CME", "ES", "FIFO", "7.0"]],
    )

    specs: CmeSpecs = load_cme_specs(tmp_path)

    # tables present
    assert set(list_tables(specs)) >= {"prf", "gpr"}

    # raw fetch preserves all fields
    prf_es = first(specs, "prf", "ES", "CME")
    assert prf_es is not None and prf_es.get("some_extra") == "hello"

    # rows by key
    gpr_rows = rows(specs, "gpr", product_code="ES", exchange="CME")
    assert len(gpr_rows) == 1
    assert gpr_rows[0]["matching_algorithm"] == "FIFO"

    # convenience math
    assert tick_size(specs, "ES", "CME") == 0.25
    assert dollars_per_tick(specs, "ES", "CME") == 12.5
