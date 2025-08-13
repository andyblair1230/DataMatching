# sierra-sync

Deterministic trade and depth file matcher for [Sierra Chart](https://www.sierrachart.com/).

`sierra-sync` locates and pairs `.scid` trade files with their corresponding market depth (`.depth`) files for a given symbol and trading day. It’s designed to be deterministic and easy to integrate into further processing pipelines.

## Installation

Clone the repo:

git clone https://github.com/andyblair1230/DataMatching.git
cd DataMatching

Install in development (editable) mode:

pip install -e .

Or build and install normally:

pip install .

Requires Python 3.12+.

## Configuration

`sierra-sync` loads settings from a YAML file.  
The config can be specified with `--config` or defaults to your `config.yaml` in the repo root.

Example `config.yaml`:

scid_root: C:\SierraChart\Data
depth_root: C:\SierraChart\Data\MarketDepthData
logs_root: C:\sierra-logs
timezone: America/New_York

- scid_root: Directory containing `.scid` trade files  
- depth_root: Directory containing `.depth` market depth files  
- logs_root: Directory for `sierra-sync` logs  
- timezone: Your Sierra Chart timezone (IANA format)  

## Commands

### Version
python -m sierra_sync version  
Outputs the installed version.

### Doctor
python -m sierra_sync doctor  
Runs basic environment and config checks, logging to `logs_root`.

### Sync
python -m sierra_sync sync ES 2025-08-12  
Attempts to locate the matching `.scid` and `.depth` files for `ES` on August 12, 2025.

#### Dry Run
python -m sierra_sync sync ES 2025-08-12 --dry-run  
Prints the plan without performing any writes.

#### Selecting a specific stem
python -m sierra_sync sync ES 2025-08-12 --dry-run --stem ESU25_FUT_CME  
Restricts matching to the provided stem.

## Logging

Logs are written to:
<logs_root>/<YYYYMMDD>/<HHMMSS>.log

## Development

Lint, format, and type-check:
pre-commit run --all-files

Run tests:
pytest

## License
Proprietary. All rights reserved.
