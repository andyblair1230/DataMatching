from __future__ import annotations

import mmap
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

# Depth header: <IIII48s  (64 bytes)
#   uint32 magic "SCDD" as 0x44444353 (little-endian to bytes "SCDD")
#   uint32 HeaderSize
#   uint32 RecordSize
#   uint32 Version
#   char[48] reserve
_DEPTH_HDR = struct.Struct("<IIII48s")

# Depth record: <QBBHfII  (24 bytes)
#   uint64 SCDateTimeUS
#   uint8  Command
#   uint8  Flags
#   uint16 NumOrders
#   float32 Price
#   uint32 Quantity
#   uint32 Reserved
_DEPTH_REC = struct.Struct("<QBBHfII")

_SCDD_MAGIC = 0x44444353  # "SCDD"


@dataclass(frozen=True)
class DepthHeader:
    header_size: int
    record_size: int
    version: int


@dataclass(frozen=True)
class DepthRecord:
    dt_us: int
    command: int
    flags: int
    num_orders: int
    price: float
    quantity: int
    reserved: int


class DepthFile:
    """Reader for Sierra Market Depth .depth files."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._fh: BinaryIO | None = None
        self._mm: mmap.mmap | None = None
        self.header: DepthHeader | None = None
        self._data_offset: int = 0
        self._n_records: int = 0

    def __enter__(self) -> DepthFile:
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
        fh = open(self.path, "rb")
        self._fh = fh
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        self._mm = mm

        if mm.size() < _DEPTH_HDR.size:
            raise ValueError("Depth file too small to contain header")

        magic, hdr_sz, rec_sz, version, _ = _DEPTH_HDR.unpack_from(mm, 0)
        if magic != _SCDD_MAGIC:
            raise ValueError(f"Bad depth magic: {hex(magic)} (expected {_SCDD_MAGIC:#x})")

        if hdr_sz != _DEPTH_HDR.size:
            # Some future-proofing; Sierra documents 64 bytes currently.
            raise ValueError(f"Unexpected depth header size {hdr_sz}")

        if rec_sz != _DEPTH_REC.size:
            raise ValueError(f"Unexpected depth record size {rec_sz} (expected {_DEPTH_REC.size})")

        self.header = DepthHeader(hdr_sz, rec_sz, version)
        self._data_offset = hdr_sz

        data_bytes = mm.size() - self._data_offset
        if data_bytes % rec_sz != 0:
            # Truncated file is still common mid-session; allow if you want by warning.
            # Here we enforce integrity.
            raise ValueError("Depth file data size is not a multiple of record size")

        self._n_records = data_bytes // rec_sz

    def close(self) -> None:
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __len__(self) -> int:
        return self._n_records

    def iter_records(self) -> Iterator[DepthRecord]:
        if self._mm is None or self.header is None:
            raise RuntimeError("DepthFile not opened")

        mm = self._mm
        rec_sz = self.header.record_size
        off = self._data_offset
        end = off + self._n_records * rec_sz

        while off < end:
            dt_us, cmd, flags, orders, price, qty, reserved = _DEPTH_REC.unpack_from(mm, off)
            yield DepthRecord(
                dt_us=dt_us,
                command=cmd,
                flags=flags,
                num_orders=orders,
                price=price,
                quantity=qty,
                reserved=reserved,
            )
            off += rec_sz
