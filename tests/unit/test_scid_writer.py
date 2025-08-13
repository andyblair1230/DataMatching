from __future__ import annotations

from pathlib import Path

import pytest

from sierra_sync.io_adapters.scid_reader import ScidFile
from sierra_sync.io_adapters.scid_writer import ScidWriter, ScidWriteRecord


def test_scid_writer_creates_and_appends(tmp_path: Path) -> None:
    f = tmp_path / "ESU25_FUT_CME.scid"

    # Write two records
    with ScidWriter(f) as w:
        w.append(
            ScidWriteRecord(
                dt_us=1000,
                open=0.0,
                high=5000.25,
                low=5000.00,
                close=5000.25,
                num_trades=1,
                total_volume=5,
                bid_volume=0,
                ask_volume=5,
            )
        )
        w.append(
            ScidWriteRecord(
                dt_us=2000,
                open=0.0,
                high=5000.50,
                low=5000.25,
                close=5000.50,
                num_trades=1,
                total_volume=3,
                bid_volume=3,
                ask_volume=0,
            )
        )

    # Read back
    with ScidFile(f) as r:
        assert len(r) == 2
        recs = list(r.iter_records())
        assert recs[0].dt_us == 1000
        assert recs[0].close == pytest.approx(5000.25)
        assert recs[0].ask_volume == 5
        assert recs[1].dt_us == 2000
        assert recs[1].bid_volume == 3


def test_scid_writer_validates_existing_header(tmp_path: Path) -> None:
    f = tmp_path / "bad.scid"
    # Create garbage file
    f.write_bytes(b"BAD!" * 16)
    with pytest.raises(ValueError):
        with ScidWriter(f):
            pass
