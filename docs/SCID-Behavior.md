SCID File Behavior
------------------


Purpose
Describe how Sierra Chart Intraday (.scid) files behave on disk so we can read them correctly and reason about timestamps, sentinels, and volumes. This document covers the on‑disk input behavior only (not the project’s rewritten outputs).

Scope
• One contract per .scid file; the file can span many days (it is not per‑day).
• Fixed header (56 bytes) + fixed-size records (40 bytes).
• Prices are stored as 4‑byte floats on disk.

Timestamp semantics (SCDateTimeMS)
• Datetime is a 64‑bit integer: microseconds since 1899‑12‑30 00:00:00 UTC (UTC required).
• Effective precision is 1 millisecond. When multiple trades share the same millisecond, the microsecond portion is used as a counter to produce unique, strictly ordered timestamps within that millisecond.

Record layout (40 bytes)
• Little‑endian "<QffffIIII"

uint64 dt_us (SCDateTimeMS)

float open, high, low, close

uint32 num_trades, total_volume, bid_volume, ask_volume

Tick vs. aggregated modes
• Tick mode (one trade per record):

open = 0.0 (sentinel meaning “single trade with bid/ask embedded”),

high = ask at trade time, low = bid at trade time, close = trade price.
• Aggregated intervals (1s/10s/1m…): OHLC/volumes for that interval; records align to interval boundaries.

Special sentinels in open (tick + CME unbundled)
• open == 0.0 → SINGLE_TRADE_WITH_BID_ASK
• open == -1.99900095e+37 → FIRST_SUB_TRADE_OF_UNBUNDLED_TRADE
• open == -1.99900197e+37 → LAST_SUB_TRADE_OF_UNBUNDLED_TRADE
Guidance: these are semantic markers, not prices; preserve exactly.

“Integer native price units” (what it means)
Some data services provide prices as integer native units (no decimal), where a tick size converts native units to human‑readable prices (e.g., native 201 with tick 0.005 → 1.005). Sierra writes prices as floats in .scid, but values may reflect those native units from the feed. This is an input characteristic to be aware of when interpreting raw values (the project’s internal scaling policy is documented elsewhere).

Write/flush behavior (input expectation)
Sierra buffers and flushes approximately every 5 seconds by default; .scid is append‑only during live operation.

What this doc intentionally does not cover
• Any rewriting/synchronization rules, BBO policy, or output file guarantees (see project docs for that).
