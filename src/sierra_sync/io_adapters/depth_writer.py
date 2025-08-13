from __future__ import annotations

import io
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

# Must mirror the reader definitions
_DEPTH_HDR = struct.Struct("<IIII48s")  # 64 bytes
_DEPTH_REC = struct.Struct("<QBBHfII")  # 24 bytes

_SCDD_MAGIC = 0x44444353  # "SCDD" as uint32 in little-endian
_HDR_SIZE = _DEPTH_HDR.size
_REC_SIZE = _DEPTH_REC.size


@dataclass(frozen=True)
class DepthWriteRecord:
    dt_us: int
    command: int
    flags: int
    num_orders: int
    price: float
    quantity: int
    reserved: int = 0  # Sierra leaves this as padding


class DepthWriter:
    """
    Minimal Market Depth (.depth) writer:
      - Creates header if needed
      - Verifies header if file exists
      - Appends records
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._fh: BinaryIO | None = None
        self._opened_new = False

    def __enter__(self) -> DepthWriter:
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
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if self.path.exists():
            fh = open(self.path, "r+b", buffering=0)
            self._opened_new = False
            self._verify_header(fh)
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

    def append(self, rec: DepthWriteRecord) -> None:
        if self._fh is None:
            raise RuntimeError("DepthWriter not opened")

        packed = _DEPTH_REC.pack(
            rec.dt_us,
            rec.command,
            rec.flags,
            rec.num_orders,
            rec.price,
            rec.quantity,
            rec.reserved,
        )
        self._fh.write(packed)

    # --- internals -------------------------------------------------

    def _write_header(self, fh: BinaryIO) -> None:
        # fields: magic, hdr_sz, rec_sz, version(=1), reserve(48*0)
        header = _DEPTH_HDR.pack(
            _SCDD_MAGIC,
            _HDR_SIZE,
            _REC_SIZE,
            1,  # version
            b"\x00" * 48,
        )
        fh.write(header)

    def _verify_header(self, fh: BinaryIO) -> None:
        fh.seek(0, io.SEEK_SET)
        blob = fh.read(_HDR_SIZE)
        if len(blob) != _HDR_SIZE:
            raise ValueError("Existing depth file too small to contain header")

        magic, hdr_sz, rec_sz, version, _reserve = _DEPTH_HDR.unpack(blob)
        if magic != _SCDD_MAGIC:
            raise ValueError(f"Bad depth magic in existing file: {magic!r}")
        if hdr_sz != _HDR_SIZE:
            raise ValueError(f"Unexpected depth header size {hdr_sz}")
        if rec_sz != _REC_SIZE:
            raise ValueError(f"Unexpected depth record size {rec_sz} (expected {_REC_SIZE})")
