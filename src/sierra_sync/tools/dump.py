from __future__ import annotations

import os
import struct
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace as _Row
from typing import BinaryIO, cast
from zoneinfo import ZoneInfo

# Keep these imports for type compatibility and any future reuse
from ..io_adapters.depth_reader import DepthRecord
from ..io_adapters.discovery import choose_best, discover_by_depth_multi
from ..io_adapters.scid_reader import ScidRecord
from ..utils.sc_time import (
    datetime_to_sc_microseconds,
    sc_microseconds_to_datetime,
)

# Depth command codes -> short tags
_CMD_TAG = {
    1: "CLR",
    2: "AB",
    3: "AA",
    4: "MB",
    5: "MA",
    6: "DB",
    7: "DA",
}
_FLAG_EOB = 0x01


def _fmt_ms(us: int) -> str:
    dt = sc_microseconds_to_datetime(us).astimezone(UTC)
    return dt.strftime("%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}"


def _fmt_us(us: int) -> str:
    return str(us)


def _ms_floor_us(us: int) -> int:
    return (us // 1000) * 1000


def _depth_side(cmd: int) -> str | None:
    if cmd in (2, 4, 6):
        return "bid"
    if cmd in (3, 5, 7):
        return "ask"
    return None


# --------- low-level file layout helpers (binary search friendly) ---------


def _depth_layout(path: Path) -> tuple[int, int, int]:
    """Return (data_offset_bytes, record_size, n_records) for a .depth file."""
    sz = os.path.getsize(path)
    with path.open("rb") as f:
        hdr = f.read(64)
        if len(hdr) < 64:
            return (0, 0, 0)
        # "<IIII48s": magic, header_size, record_size, version, reserved
        _magic, header_size, record_size, _version, _resv = struct.unpack("<IIII48s", hdr)
    data_off = header_size
    if record_size <= 0 or sz <= data_off:
        return (data_off, record_size, 0)
    nrecs = (sz - data_off) // record_size
    return (data_off, record_size, int(nrecs))


def _scid_layout(path: Path) -> tuple[int, int, int]:
    """Return (data_offset_bytes, record_size, n_records) for a .scid file."""
    sz = os.path.getsize(path)
    with path.open("rb") as f:
        hdr = f.read(56)
        if len(hdr) < 56:
            return (0, 0, 0)
        # "<4sIIHHI36s": 'SCID', header_size, record_size, ...
        _magic, header_size, record_size, _u1, _u2, _utcstart, _resv = struct.unpack("<4sIIHHI36s", hdr)
    data_off = header_size
    if record_size <= 0 or sz <= data_off:
        return (data_off, record_size, 0)
    nrecs = (sz - data_off) // record_size
    return (data_off, record_size, int(nrecs))


def _bisect_left_dt(f: BinaryIO, data_off: int, rec_size: int, nrecs: int, target_us: int) -> int:
    """
    Return the smallest index i in [0, nrecs) such that dt_us[i] >= target_us.
    Only reads the first 8 bytes (uint64 dt_us) of each record for compare.
    """
    lo, hi = 0, nrecs
    while lo < hi:
        mid = (lo + hi) // 2
        f.seek(data_off + mid * rec_size, os.SEEK_SET)
        b = f.read(8)
        if len(b) < 8:
            hi = mid
            continue
        (dt_mid,) = struct.unpack("<Q", b)
        if dt_mid < target_us:
            lo = mid + 1
        else:
            hi = mid
    return lo


# ----------------- high-level readers (binary-search windowed) -----------------


def _depth_records_in_window(path: Path, start_us: int, end_us: int) -> list[DepthRecord]:
    data_off, rec_size, nrecs = _depth_layout(path)
    if nrecs == 0:
        return []
    out: list[DepthRecord] = []
    with path.open("rb") as f:
        i = _bisect_left_dt(f, data_off, rec_size, nrecs, start_us)
        fmt = struct.Struct("<QBBHfII")  # dt_us, cmd, flags, num_orders, price, qty, reserved
        while i < nrecs:
            f.seek(data_off + i * rec_size, os.SEEK_SET)
            buf = f.read(rec_size)
            if len(buf) < rec_size:
                break
            dt_us, command, flags, num_orders, price, quantity, reserved = fmt.unpack_from(buf)
            if dt_us >= end_us:
                break
            out.append(
                cast(
                    DepthRecord,
                    _Row(
                        dt_us=dt_us,
                        command=command,
                        flags=flags,
                        num_orders=num_orders,
                        price=price,
                        quantity=quantity,
                        reserved=reserved,
                    ),
                )
            )
            i += 1
    return out


def _depth_snapshot_at_or_after(path: Path, at_us: int) -> list[DepthRecord]:
    """Find the next CLEAR_BOOK batch at/after at_us and return that batch."""
    data_off, rec_size, nrecs = _depth_layout(path)
    if nrecs == 0:
        return []
    fmt = struct.Struct("<QBBHfII")
    with path.open("rb") as f:
        i = _bisect_left_dt(f, data_off, rec_size, nrecs, at_us)
        # advance to first CLEAR_BOOK at/after at_us
        while i < nrecs:
            f.seek(data_off + i * rec_size, os.SEEK_SET)
            buf = f.read(rec_size)
            if len(buf) < rec_size:
                return []
            dt_us, command, flags, num_orders, price, quantity, reserved = fmt.unpack_from(buf)
            if dt_us >= at_us and command == 1:  # CLEAR_BOOK
                batch: list[DepthRecord] = []
                # collect this record and subsequent until EOB
                while i < nrecs:
                    if len(buf) < rec_size:
                        break
                    dt_us, command, flags, num_orders, price, quantity, reserved = fmt.unpack_from(buf)
                    batch.append(
                        cast(
                            DepthRecord,
                            _Row(
                                dt_us=dt_us,
                                command=command,
                                flags=flags,
                                num_orders=num_orders,
                                price=price,
                                quantity=quantity,
                                reserved=reserved,
                            ),
                        )
                    )

                    if (flags & _FLAG_EOB) != 0:
                        return batch
                    i += 1
                    f.seek(data_off + i * rec_size, os.SEEK_SET)
                    buf = f.read(rec_size)
                return batch
            i += 1
    return []


def _scid_records_in_window(path: Path, start_us: int, end_us: int) -> list[ScidRecord]:
    data_off, rec_size, nrecs = _scid_layout(path)
    if nrecs == 0:
        return []
    out: list[ScidRecord] = []
    with path.open("rb") as f:
        i = _bisect_left_dt(f, data_off, rec_size, nrecs, start_us)
        fmt = struct.Struct("<QffffIIII")  # dt_us, open, high, low, close, num_trades, total_vol, bid_vol, ask_vol
        while i < nrecs:
            f.seek(data_off + i * rec_size, os.SEEK_SET)
            buf = f.read(rec_size)
            if len(buf) < rec_size:
                break
            (dt_us, open_, high, low, close, ntrd, vol, bidv, askv) = fmt.unpack_from(buf)
            if dt_us >= end_us:
                break
            out.append(
                cast(
                    ScidRecord,
                    _Row(
                        dt_us=dt_us,
                        open=open_,
                        high=high,
                        low=low,
                        close=close,
                        num_trades=ntrd,
                        total_volume=vol,
                        bid_volume=bidv,
                        ask_volume=askv,
                    ),
                )
            )
            i += 1
    return out


# ----------------------------- rendering helpers -----------------------------


def _md_table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> str:
    line1 = "| " + " | ".join(headers) + " |"
    line2 = "| " + " | ".join("---" for _ in headers) + " |"
    lines = [line1, line2]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def depth_to_markdown(records: list[DepthRecord]) -> str:
    headers = ["t(ms)", "cmd", "side", "price", "qty", "flags"]
    rows: list[list[str]] = []
    for rec in records:
        rows.append(
            [
                _fmt_ms(rec.dt_us),
                _CMD_TAG.get(rec.command, str(rec.command)),
                _depth_side(rec.command) or "",
                f"{rec.price:.5f}",
                f"{rec.quantity}",
                "EOB" if (rec.flags & _FLAG_EOB) else "",
            ]
        )
    return _md_table(headers, rows)


def scid_to_markdown(records: list[ScidRecord]) -> str:
    headers = ["t(us UTC)", "open", "high", "low", "close", "trd", "vol", "bidv", "askv"]
    rows: list[list[str]] = []
    for rec in records:
        rows.append(
            [
                _fmt_us(rec.dt_us),
                f"{rec.open:.5f}",
                f"{rec.high:.5f}",
                f"{rec.low:.5f}",
                f"{rec.close:.5f}",
                f"{rec.num_trades}",
                f"{rec.total_volume}",
                f"{rec.bid_volume}",
                f"{rec.ask_volume}",
            ]
        )
    return _md_table(headers, rows)


def both_window_markdown_full(
    depth_records: list[DepthRecord],
    scid_records: list[ScidRecord],
) -> str:
    """
    Always produce two sections:
      1) Full depth table for the window (or _none_)
      2) Full SCID table for the window (or _none_)
    No grouping, no capping.
    """
    parts: list[str] = []
    parts.append("## Depth\n")
    parts.append(depth_to_markdown(depth_records) if depth_records else "_none_")
    parts.append("")
    parts.append("## SCID (full window)\n")
    parts.append(scid_to_markdown(scid_records) if scid_records else "_none_")
    return "\n".join(parts)


# ----------------------- ZIP/BY-MILLISECOND SIDE-BY-SIDE ---------------------


def _depth_group_by_ms(records: Iterable[DepthRecord]) -> dict[int, list[DepthRecord]]:
    groups: dict[int, list[DepthRecord]] = {}
    for rec in records:
        key = (rec.dt_us // 1000) * 1000
        groups.setdefault(key, []).append(rec)
    return groups


def scid_group_by_ms(records: Iterable[ScidRecord]) -> dict[int, list[ScidRecord]]:
    groups: dict[int, list[ScidRecord]] = {}
    for rec in records:
        key = (rec.dt_us // 1000) * 1000
        groups.setdefault(key, []).append(rec)
    return groups


def both_window_markdown_side_by_side(
    depth_records: list[DepthRecord],
    scid_records: list[ScidRecord],
) -> str:
    """
    Join Depth and SCID by *millisecond*. For each ms in the *intersection*,
    zip the per-ms lists row-wise (fill the shorter side with blanks).
    """
    d_by = _depth_group_by_ms(depth_records)
    s_by = scid_group_by_ms(scid_records)
    common = sorted(set(d_by.keys()) & set(s_by.keys()))
    if not common:
        return "_No milliseconds with both Depth and SCID in this window._"

    headers = [
        "t(ms)",
        "d_cmd",
        "d_side",
        "d_price",
        "d_qty",
        "d_flags",
        "s_t(us)",
        "s_open",
        "s_high",
        "s_low",
        "s_close",
        "s_trd",
        "s_vol",
        "s_bidv",
        "s_askv",
    ]
    rows: list[list[str]] = []

    for ms in common:
        d_list = d_by.get(ms, [])
        s_list = s_by.get(ms, [])
        L = max(len(d_list), len(s_list))
        for i in range(L):
            d = d_list[i] if i < len(d_list) else None
            s = s_list[i] if i < len(s_list) else None
            rows.append(
                [
                    _fmt_ms(ms),
                    (_CMD_TAG.get(d.command, str(d.command)) if d else ""),
                    (_depth_side(d.command) if d else "") or "",
                    (f"{d.price:.5f}" if d else ""),
                    (f"{d.quantity}" if d else ""),
                    ("EOB" if (d and (d.flags & _FLAG_EOB)) else ""),
                    (_fmt_us(s.dt_us) if s else ""),
                    (f"{s.open:.5f}" if s else ""),
                    (f"{s.high:.5f}" if s else ""),
                    (f"{s.low:.5f}" if s else ""),
                    (f"{s.close:.5f}" if s else ""),
                    (f"{s.num_trades}" if s else ""),
                    (f"{s.total_volume}" if s else ""),
                    (f"{s.bid_volume}" if s else ""),
                    (f"{s.ask_volume}" if s else ""),
                ]
            )
    return _md_table(headers, rows)


def _fmt_depth_inline(rec: DepthRecord) -> str:
    tag = _CMD_TAG.get(rec.command, str(rec.command))
    side = _depth_side(rec.command) or ""
    eob = " EOB" if (rec.flags & _FLAG_EOB) else ""
    return f"{tag:<3} {side:<3} {rec.price:.5f} x {rec.quantity}{eob}"


def _fmt_scid_inline(rec: ScidRecord) -> str:
    us_in_ms = rec.dt_us % 1000
    return f"{us_in_ms:03d}us close={rec.close:.5f} " f"(ask={rec.high:.5f}/bid={rec.low:.5f}) " f"trd={rec.num_trades} vol={rec.total_volume}"


def trades_window_markdown_side_by_side(
    scid_records: list[ScidRecord],
    depth_records: list[DepthRecord],
) -> str:
    """
    For each millisecond with >=1 SCID:
      Left = all depth rows in that ms
      Right = all SCID rows in that ms
    """
    if not scid_records and not depth_records:
        return "_none_"

    scid_by_ms = scid_group_by_ms(scid_records)
    depth_by_ms = _depth_group_by_ms(depth_records)

    parts: list[str] = []
    for ms in sorted(scid_by_ms.keys()):
        left = depth_by_ms.get(ms, [])
        right = scid_by_ms[ms]

        parts.append(f"### {_fmt_ms(ms)}")
        headers = ["Depth @ ms", "SCID @ ms"]
        max_len = max(len(left), len(right))
        rows: list[list[str]] = []
        for i in range(max_len):
            left_cell = _fmt_depth_inline(left[i]) if i < len(left) else ""
            right_cell = _fmt_scid_inline(right[i]) if i < len(right) else ""
            rows.append([left_cell, right_cell])
        parts.append(_md_table(headers, rows))
        parts.append("")

    return "\n".join(parts)


# ------------------------------ discovery & time -----------------------------


def resolve_files(
    sc_root: Path,
    depth_root: Path,
    symbol: str,
    day_utc: datetime,
) -> tuple[Path | None, Path | None, str | None]:
    cands = discover_by_depth_multi(sc_root, depth_root, symbol, day_utc.date(), 7)
    if not cands:
        return (None, None, None)
    chosen = choose_best(cands)
    assert chosen is not None
    return (
        Path(chosen.scid_file) if chosen.scid_file else None,
        Path(chosen.depth_file),
        chosen.stem,
    )


def parse_hms_ms(hms: str) -> tuple[int, int, int, int]:
    part = hms.strip()
    if "." in part:
        hhmmss, ms = part.split(".", 1)
        ms_i = int(ms[:3].ljust(3, "0"))
    else:
        hhmmss = part
        ms_i = 0
    hh, mm, ss = [int(x) for x in hhmmss.split(":")]
    return hh, mm, ss, ms_i


def window_us_for_day(day_utc: datetime, start_hms: str, end_hms: str) -> tuple[int, int]:
    h1, m1, s1, ms1 = parse_hms_ms(start_hms)
    h2, m2, s2, ms2 = parse_hms_ms(end_hms)
    start_dt = day_utc.replace(hour=h1, minute=m1, second=s1, microsecond=ms1 * 1000, tzinfo=UTC)
    end_dt = day_utc.replace(hour=h2, minute=m2, second=s2, microsecond=ms2 * 1000, tzinfo=UTC)
    return (datetime_to_sc_microseconds(start_dt), datetime_to_sc_microseconds(end_dt))


def window_us_for_local_day(
    day_local: date,
    tz_name: str,
    start_local: str,
    end_local: str,
) -> tuple[int, int]:
    tz = ZoneInfo(tz_name)
    h1, m1, s1, ms1 = parse_hms_ms(start_local)
    h2, m2, s2, ms2 = parse_hms_ms(end_local)

    start_local_dt = datetime(day_local.year, day_local.month, day_local.day, h1, m1, s1, ms1 * 1000, tzinfo=tz)
    end_local_dt = datetime(day_local.year, day_local.month, day_local.day, h2, m2, s2, ms2 * 1000, tzinfo=tz)

    start_utc = start_local_dt.astimezone(UTC)
    end_utc = end_local_dt.astimezone(UTC)
    return (datetime_to_sc_microseconds(start_utc), datetime_to_sc_microseconds(end_utc))


def snapshot_start_us_for_day(day_utc: datetime, at_hms: str) -> int:
    h, m, s, ms = parse_hms_ms(at_hms)
    at_dt = day_utc.replace(hour=h, minute=m, second=s, microsecond=ms * 1000, tzinfo=UTC)
    return datetime_to_sc_microseconds(at_dt)


# ------------------------------ trade helper views ---------------------------


def _depth_records_at_ms(path: Path, ms_us: int) -> list[DepthRecord]:
    """All depth rows whose timestamp falls in this exact millisecond."""
    return _depth_records_in_window(path, ms_us, ms_us + 1000)


def trades_window_markdown(
    scid_records: list[ScidRecord],
    depth_path: Path,
) -> str:
    """
    Legacy: SCID full table, then depth sections per trade-ms.
    (Kept for reference; not used by CLI 'trades' anymore.)
    """
    parts: list[str] = []
    parts.append("## SCID (full window)\n")
    parts.append(scid_to_markdown(scid_records) if scid_records else "_none_")
    parts.append("")

    if not scid_records:
        parts.append("## Depth @ trade timestamps\n_none_")
        return "\n".join(parts)

    parts.append("## Depth @ trade timestamps\n")
    by_ms = scid_group_by_ms(scid_records)
    for ms in sorted(by_ms.keys()):
        depth_rows = _depth_records_at_ms(depth_path, ms)
        parts.append(f"### {_fmt_ms(ms)}")
        parts.append(depth_to_markdown(depth_rows) if depth_rows else "_none_")
        parts.append("")

    return "\n".join(parts)
