from __future__ import annotations

import mmap
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

# SCID header: <4sIIHHI36s  (56 bytes)
#   "SCID"
#   uint32 HeaderSize
#   uint32 RecordSize
#   uint16 Version
#   uint16 Unused1
#   uint32 UTCStartIndex (should be 0)
#   char[36] reserve
_SCID_HDR = struct.Struct("<4sIIHHI36s")

# SCID record: <QffffIIII  (40 bytes)
#   uint64 SCDateTimeUS
#   float32 Open, High, Low, Close
#   uint32 NumTrades, TotalVolume, BidVolume, AskVolume
_SCID_REC = struct.Struct("<QffffIIII")


@dataclass(frozen=True)
class ScidHeader:
    header_size: int
    record_size: int
    version: int
    utc_start_index: int


@dataclass(frozen=True)
class ScidRecord:
    dt_us: int
    open: float
    high: float
    low: float
    close: float
    num_trades: int
    total_volume: int
    bid_volume: int
    ask_volume: int


class ScidFile:
    """Reader for Sierra Intraday .scid files."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._fh: BinaryIO | None = None
        self._mm: mmap.mmap | None = None
        self.header: ScidHeader | None = None
        self._data_offset: int = 0
        self._n_records: int = 0

    def __enter__(self) -> ScidFile:
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

        if mm.size() < _SCID_HDR.size:
            raise ValueError("SCID file too small to contain header")

        magic, hdr_sz, rec_sz, version, unused1, utc_start, _ = _SCID_HDR.unpack_from(mm, 0)
        if magic != b"SCID":
            raise ValueError(f"Bad SCID magic: {magic!r}")

        if hdr_sz != _SCID_HDR.size:
            raise ValueError(f"Unexpected SCID header size {hdr_sz}")

        if rec_sz != _SCID_REC.size:
            raise ValueError(f"Unexpected SCID record size {rec_sz} (expected {_SCID_REC.size})")

        self.header = ScidHeader(hdr_sz, rec_sz, version, utc_start)
        self._data_offset = hdr_sz

        data_bytes = mm.size() - self._data_offset
        if data_bytes % rec_sz != 0:
            raise ValueError("SCID file data size is not a multiple of record size")

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

    def iter_records(self) -> Iterator[ScidRecord]:
        if self._mm is None or self.header is None:
            raise RuntimeError("ScidFile not opened")

        mm = self._mm
        rec_sz = self.header.record_size
        off = self._data_offset
        end = off + self._n_records * rec_sz

        while off < end:
            (
                dt_us,
                opn,
                high,
                low,
                close,
                ntr,
                tvol,
                bvol,
                avol,
            ) = _SCID_REC.unpack_from(mm, off)
            yield ScidRecord(
                dt_us=dt_us,
                open=opn,
                high=high,
                low=low,
                close=close,
                num_trades=ntr,
                total_volume=tvol,
                bid_volume=bvol,
                ask_volume=avol,
            )
            off += rec_sz
