Depth File Behavior
-------------------

Purpose
Describe how Sierra Chart Market Depth (.depth) files behave on disk so we can parse headers, commands, batches, and timestamps correctly. This document covers the on‑disk input behavior only.

Scope
• One UTC day per file, one contract per file. Rolls at 00:00:00 UTC.
• Fixed header (64 bytes) + fixed-size records (24 bytes).
• Prices are stored as 4‑byte floats on disk.

Timestamp semantics (SCDateTimeMS)
• Datetime is a 64‑bit integer microseconds since 1899‑12‑30 00:00:00 UTC (UTC required).
• Effective precision is 1 millisecond. Multiple batches can share the same millisecond timestamp; maintain their written order. Batch boundaries are marked with END_OF_BATCH.

Header (64 bytes)
• Magic "SCDD"; validate HeaderSize=64, RecordSize=24. LE struct "<IIII48s".

Record (24 bytes)
• Little‑endian "<QBBHfII"

uint64 dt_us (SCDateTimeMS)

uint8 command, uint8 flags, uint16 num_orders

float price, uint32 quantity, uint32 reserved (padding)
Commands: 1=CLEAR_BOOK, 2=ADD_BID_LEVEL, 3=ADD_ASK_LEVEL, 4=MODIFY_BID_LEVEL, 5=MODIFY_ASK_LEVEL, 6=DELETE_BID_LEVEL, 7=DELETE_ASK_LEVEL.
Flag: 0x01 = END_OF_BATCH.

Snapshot behavior (periodic)
A snapshot is a batch with this pattern:

CLEAR_BOOK → 2) add all bid levels (best→worse) → 3) add all ask levels (worse→best) → 4) END_OF_BATCH. Snapshots occur periodically (about every ~10 minutes).

Batch ordering rules (within the same timestamp)
• Multiple batches may share the exact same timestamp; keep occurrence order.
• Within a batch: all bid updates come first; ask updates follow; one side may be absent.

Invariants & edge cases
• Data region size must be a multiple of 24 bytes; otherwise the file is truncated/corrupted.
• No order IDs; levels are per‑price with NumOrders and Quantity.
• Preserve reserved=0 when writing.

What this doc intentionally does not cover
• Any rewriting/synchronization rules, BBO policy, or output file guarantees (see project docs for that).
