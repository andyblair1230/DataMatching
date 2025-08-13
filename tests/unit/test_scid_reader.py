from __future__ import annotations

from pathlib import Path

from sierra_sync.io_adapters.scid_reader import _SCID_HDR, _SCID_REC, ScidFile


def write_scid_file(path: Path) -> None:
    hdr = _SCID_HDR.pack(b"SCID", _SCID_HDR.size, _SCID_REC.size, 1, 0, 0, b"\x00" * 36)
    rec1 = _SCID_REC.pack(2000000, 1.0, 2.0, 0.5, 1.5, 1, 100, 60, 40)
    rec2 = _SCID_REC.pack(3000000, 1.6, 2.1, 0.6, 1.7, 2, 120, 70, 50)
    path.write_bytes(hdr + rec1 + rec2)


def test_scid_iter(tmp_path: Path) -> None:
    f = tmp_path / "Y.scid"
    write_scid_file(f)

    with ScidFile(f) as sf:
        assert len(sf) == 2
        rows = list(sf.iter_records())
        assert rows[0].open == 1.0
        assert rows[0].high == 2.0
        assert rows[0].low == 0.5
        assert rows[0].close == 1.5
        assert rows[0].total_volume == 100
        assert rows[1].num_trades == 2
