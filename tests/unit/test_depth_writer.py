from __future__ import annotations

from pathlib import Path

import pytest

from sierra_sync.io_adapters.depth_reader import DepthFile
from sierra_sync.io_adapters.depth_writer import DepthWriter, DepthWriteRecord


def test_depth_writer_creates_and_appends(tmp_path: Path) -> None:
    f = tmp_path / "ESU25_FUT_CME.2025-08-12.depth"

    # Two simple records: add-bid and add-ask around same time
    with DepthWriter(f) as w:
        w.append(
            DepthWriteRecord(
                dt_us=1_000,
                command=2,  # ADD_BID_LEVEL
                flags=0,
                num_orders=3,
                price=5000.00,
                quantity=15,
            )
        )
        w.append(
            DepthWriteRecord(
                dt_us=2_000,
                command=3,  # ADD_ASK_LEVEL
                flags=1,  # FLAG_END_OF_BATCH (optional)
                num_orders=2,
                price=5000.25,
                quantity=8,
            )
        )

    # Read back and verify
    with DepthFile(f) as r:
        assert len(r) == 2
        rows = list(r.iter_records())
        assert rows[0].dt_us == 1_000
        assert rows[0].command == 2
        assert rows[0].price == pytest.approx(5000.00)
        assert rows[0].quantity == 15

        assert rows[1].dt_us == 2_000
        assert rows[1].command == 3
        assert rows[1].flags == 1
        assert rows[1].price == pytest.approx(5000.25)
        assert rows[1].quantity == 8


def test_depth_writer_validates_existing_header(tmp_path: Path) -> None:
    f = tmp_path / "bad.depth"
    f.write_bytes(b"BAD!" * 16)
    with pytest.raises(ValueError):
        with DepthWriter(f):
            pass
