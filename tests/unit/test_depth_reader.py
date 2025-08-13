from __future__ import annotations

from pathlib import Path

from sierra_sync.io_adapters.depth_reader import (
    _DEPTH_HDR,
    _DEPTH_REC,
    _SCDD_MAGIC,
    DepthFile,
)


def write_depth_file(path: Path) -> None:
    # header
    hdr = _DEPTH_HDR.pack(_SCDD_MAGIC, _DEPTH_HDR.size, _DEPTH_REC.size, 1, b"\x00" * 48)
    # two records
    rec1 = _DEPTH_REC.pack(123456789, 2, 0, 3, 5000.25, 10, 0)
    rec2 = _DEPTH_REC.pack(123456999, 3, 1, 2, 5000.50, 7, 0)
    path.write_bytes(hdr + rec1 + rec2)


def test_depth_iter(tmp_path: Path) -> None:
    f = tmp_path / "X.depth"
    write_depth_file(f)

    with DepthFile(f) as df:
        assert len(df) == 2
        rows = list(df.iter_records())
        assert rows[0].dt_us == 123456789
        assert rows[0].command == 2
        assert rows[0].num_orders == 3
        assert rows[0].price == 5000.25
        assert rows[0].quantity == 10
        assert rows[1].flags == 1
        assert rows[1].price == 5000.5
