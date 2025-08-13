from __future__ import annotations

import io
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

# Header/record shapes must match readers
_SCID_HDR = struct.Struct("<4sIIHHI36s")
_SCID_REC = struct.Struct("<QffffIIII")

_MAGIC = b"SCID"
_HDR_SIZE = _SCID_HDR.size  # 56
_REC_SIZE = _SCID_REC.size  # 40


@dataclass(frozen=True)
class ScidWriteRecord:
    dt_us: int
    open: float
    high: float
    low: float
    close: float
    num_trades: int
    total_volume: int
    bid_volume: int
    ask_volume: int


class ScidWriter:
    """
    Minimal SCID writer:
      - Creates header if needed
      - Verifies header if file exists
      - Appends records
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._fh: BinaryIO | None = None
        self._opened_new = False

    def __enter__(self) -> ScidWriter:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    # --- public API ------------------------------------------------

    def open(self) -> None:
        # Create parent dirs
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Open for r+b if exists, else create new w+b then write header.
        if self.path.exists():
            fh = open(self.path, "r+b", buffering=0)
            self._opened_new = False
            self._verify_header(fh)
            # position at EOF
            fh.seek(0, io.SEEK_END)
        else:
            fh = open(self.path, "w+b", buffering=0)
            self._opened_new = True
            self._write_header(fh)

        self._fh = fh

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None

    def append(self, rec: ScidWriteRecord) -> None:
        if self._fh is None:
            raise RuntimeError("ScidWriter not opened")

        packed = _SCID_REC.pack(
            rec.dt_us,
            rec.open,
            rec.high,
            rec.low,
            rec.close,
            rec.num_trades,
            rec.total_volume,
            rec.bid_volume,
            rec.ask_volume,
        )
        self._fh.write(packed)

    # --- internals -------------------------------------------------

    def _write_header(self, fh: BinaryIO) -> None:
        # fields: magic, hdr_sz, rec_sz, version(=1), unused1(=0),
        # utc_start_index(=0), reserve(36*0)
        header = _SCID_HDR.pack(
            _MAGIC,
            _HDR_SIZE,
            _REC_SIZE,
            1,  # version
            0,  # unused1
            0,  # UTCStartIndex
            b"\x00" * 36,
        )
        fh.write(header)

    def _verify_header(self, fh: BinaryIO) -> None:
        fh.seek(0, io.SEEK_SET)
        blob = fh.read(_HDR_SIZE)
        if len(blob) != _HDR_SIZE:
            raise ValueError("Existing SCID file too small to contain header")
        magic, hdr_sz, rec_sz, version, _unused1, _utc_start, _res = _SCID_HDR.unpack(blob)
        if magic != _MAGIC:
            raise ValueError(f"Bad SCID magic in existing file: {magic!r}")
        if hdr_sz != _HDR_SIZE:
            raise ValueError(f"Unexpected SCID header size {hdr_sz}")
        if rec_sz != _REC_SIZE:
            raise ValueError(f"Unexpected SCID record size {rec_sz} (expected {_REC_SIZE})")
