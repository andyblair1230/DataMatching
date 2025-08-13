from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# Sierra “SCDateTimeMS” integer = microseconds since 1899-12-30 00:00:00 UTC
_SC_EPOCH = datetime(1899, 12, 30, tzinfo=UTC)


def sc_microseconds_to_datetime(us: int) -> datetime:
    """Convert SC microseconds since 1899-12-30 UTC to a timezone-aware datetime (UTC)."""
    return _SC_EPOCH + timedelta(microseconds=us)


def datetime_to_sc_microseconds(dt: datetime) -> int:
    """Convert timezone-aware datetime to SC microseconds. dt must be UTC."""
    if dt.tzinfo is None or dt.tzinfo is not UTC:
        dt = dt.astimezone(UTC)
    delta = dt - _SC_EPOCH
    return int(delta.total_seconds() * 1_000_000)


@dataclass(frozen=True)
class ScInstant:
    """Light wrapper if you want a typed field in dataclasses for SC timestamps."""

    micros: int

    def to_datetime(self) -> datetime:
        return sc_microseconds_to_datetime(self.micros)
