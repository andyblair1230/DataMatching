Here’s the updated plain-text `README.md` again, now with a **Current Status / Roadmap** section at the end so readers know what’s implemented vs still planned.

---

# sierra-sync

Deterministic trade and depth synchronizer and rewriter for Sierra Chart.

`sierra-sync` ingests a contract’s .scid intraday file and its per-day .depth market depth files, then produces new Sierra-compatible outputs where trades and depth updates are aligned on a single, strictly ordered timeline.

* Depth files are effectively millisecond-precision. Batches may share the same millisecond; within a batch bids come first, asks second.
* SCID files store microseconds, but in practice those microseconds act as a sequence counter within the millisecond rather than true sub-millisecond timing.

The synchronizer performs:

1. Normalize time: shave off the “fake” microsecond portion from .scid records.
2. Align to depth: match each trade to its correct depth update batch at the millisecond level.
3. Reassign microseconds: assign new sub-millisecond offsets so trades and depth interleave deterministically.
4. Overwrite BBO: set .scid high/low from the reconstructed best ask/bid in depth (depth is the BBO source of truth).
5. Inject depth events: insert depth-only updates into the .scid stream, so the two files cover the same event sequence.
6. Preserve trade bundles: use the bundled trade size and bundled number of trades stored in the .scid `open` field to compare against depth entries and pinpoint the exact placement of each trade at its price. This ensures the sequence of depth updates is true and the original trade sequence remains intact.
7. Write new outputs:

   * <stem>-SYNC.scid
   * <stem>-SYNC.<YYYY-MM-DD>.depth

This produces .scid files that reflect both trades and order book changes, synchronized with the depth feed. The result is a unified event timeline suitable for accurate replay, analytics, and charting.

## Installation

Clone the repo:

git clone [https://github.com/andyblair1230/DataMatching.git](https://github.com/andyblair1230/DataMatching.git)
cd DataMatching

Install in development (editable) mode:

pip install -e .

Or build and install normally:

pip install .

Requires Python 3.12+.

## Configuration

`sierra-sync` loads settings from a YAML file.
The config can be specified with --config or defaults to your config.yaml in the repo root.

Example config.yaml:

scid\_root: C:\SierraChart\Data
depth\_root: C:\SierraChart\Data\MarketDepthData
logs\_root: C:\sierra-logs
timezone: America/New\_York

* scid\_root: Directory containing .scid trade files
* depth\_root: Directory containing .depth market depth files
* logs\_root: Directory for sierra-sync logs
* timezone: Your Sierra Chart timezone (IANA format)

## Commands

### Dry run (plan only)

Plan the files that would be used for a symbol and trading day. This inspects depth filenames to pick the exact contract stem.

python -m sierra\_sync sync ES 2025-08-12 --dry-run

Example output:

INFO sierra\_sync.sync: sync\_plan\_multi
Candidates:

* ESU25\_FUT\_CME | ESU25\_FUT\_CME.2025-08-12.depth (exists) | C:\SierraChart\Data\ESU25\_FUT\_CME.scid

Chosen:
Contract ID: ESU25\_FUT\_CME
SCID file:   C:\SierraChart\Data\ESU25\_FUT\_CME.scid
Depth file:  C:\SierraChart\Data\MarketDepthData\ESU25\_FUT\_CME.2025-08-12.depth (exists)

### Forcing a specific contract

If multiple contracts are active for the date, you can force the stem explicitly:

python -m sierra\_sync sync ES 2025-08-12 --dry-run --stem ESU25\_FUT\_CME

### Real run (WIP)

Without --dry-run the pipeline will perform actual processing (once implemented). For now it validates inputs and exits.

python -m sierra\_sync sync ES 2025-08-12

### Config file

You can point to a YAML config explicitly (otherwise defaults are used):

python -m sierra\_sync sync ES 2025-08-12 --config C:\path\to\settings.yaml

Example YAML:

scid\_root: C:\SierraChart\Data
depth\_root: C:\SierraChart\Data\MarketDepthData
logs\_root: C:\sierra-logs
timezone: America/New\_York

### Version

python -m sierra\_sync version

Outputs the installed version.

### Doctor

python -m sierra\_sync doctor

Runs basic environment and config checks, logging to logs\_root.

### Sync

python -m sierra\_sync sync ES 2025-08-12

Attempts to locate the matching .scid and .depth files for ES on August 12, 2025.

#### Dry Run

python -m sierra\_sync sync ES 2025-08-12 --dry-run

Prints the plan without performing any writes.

#### Selecting a specific stem

python -m sierra\_sync sync ES 2025-08-12 --dry-run --stem ESU25\_FUT\_CME

Restricts matching to the provided stem.

## Logging

Logs are written to:

\<logs\_root>/<YYYYMMDD>/<HHMMSS>.log

## Development

Lint, format, and type-check:

pre-commit run --all-files

Run tests:

pytest

## License

Proprietary. All rights reserved.

## Current Status / Roadmap

* Matching and rewrite logic is under active development.
* Multi-contract handling for a single instrument will be added.
* Multi-instrument processing will be supported, with awareness of contract expiration and roll dates.
* Price scaling will become instrument-aware, using tick size metadata instead of a fixed 100x convention.
* Additional output formats (CSV/Parquet) are planned for downstream analytics.

---
