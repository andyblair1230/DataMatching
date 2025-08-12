Sierra Market Data Stream Matching Script



-Purpose:

Build a deterministic pipeline that ingests Sierra Chart trade and depth binaries, aligns their event sequences for historical and live data, and writes matched outputs back in Sierra-compatible formats.


-Inputs:

SCID intraday files and daily depth binaries. Live mode tails the same files while they grow.


-Outputs:

Matched records written to SCID and depth files for Sierra to read. Optional live mirror via DTC client later.


-Hard requirements:

Deterministic results for identical inputs. Idempotent re-runs. Internal time unit is integer nanoseconds since Unix epoch. One trade is matched at most once. Side, volume deltas, and record order are preserved. Same inputs produce the same outputs regardless of chunking.


-Non-goals:

No analytics, indicators, or ML features. No charting. No broker integration beyond Sierra file formats or DTC mirror.


-Success criteria:

On a fixed set of symbols and days, total volume and order count per timestamp reconcile exactly between trades and depth deltas. Zero unmatched trades except explicitly logged anomalies. End-to-end processing for one full RTH session completes within a target wall time you will set after profiling.


-Constraints:

Windows 10 environment. Python 3.12. Data stored outside the repo. Paths and parameters come from config, never hardcoded.


-Verification:

A verifier re-reads written SCID and depth, checks header integrity, record count, timestamp monotonicity, and checksum of segment manifests.


-Risks to watch:

File sharing semantics on Windows. Clock conversions between Sierra time and epoch. Depth anomalies that resemble cancels. Memory spikes on large candidate windows.