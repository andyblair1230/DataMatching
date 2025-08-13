from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sierra_sync.utils.sc_time import (
    datetime_to_sc_microseconds,
    sc_microseconds_to_datetime,
)


def test_roundtrip_epoch() -> None:
    dt = datetime(2025, 8, 12, 14, 30, 0, tzinfo=UTC)
    us = datetime_to_sc_microseconds(dt)
    back = sc_microseconds_to_datetime(us)
    assert abs(back - dt) < timedelta(microseconds=1)
