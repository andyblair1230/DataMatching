from __future__ import annotations

import csv
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ---------------------- normalization helpers ----------------------


def _norm_cols(cols: Iterable[str]) -> list[str]:
    out: list[str] = []
    for c in cols:
        c2 = re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
        out.append(c2)
    return out


def _norm_keys(row: dict[str, Any]) -> dict[str, Any]:
    r = {**row}
    # normalize exchange -> "exchange"
    for k in ("exchange", "exch"):
        if k in r and r[k] is not None and r[k] != "":
            r["exchange"] = str(r[k]).strip().upper()
            break
    # normalize product code -> "product_code"
    for k in (
        "product_code",
        "sym",
        "symbol",
        "globex_product_code",
        "prod_code",
        "product",
    ):
        if k in r and r[k] is not None and r[k] != "":
            r["product_code"] = str(r[k]).replace(" ", "").strip().upper()
            break
    return r


def _read_csv(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        try:
            raw_headers = next(reader)
        except StopIteration:
            return rows
        headers = _norm_cols(raw_headers)
        for raw in reader:
            if not raw:
                continue
            # pad short rows
            if len(raw) < len(headers):
                raw = raw + [""] * (len(headers) - len(raw))
            rec = {h: v for h, v in zip(headers, raw, strict=False)}
            rows.append(_norm_keys(rec))
    return rows


def _to_float(v: Any, default: float | None = None) -> float | None:
    try:
        return float(v)
    except Exception:
        return default


# ---------------------- public data model ----------------------


TableName = str  # "prf" | "gpr" | "nrr" | "spl" | "position_limits"
Key = tuple[str, str]  # (exchange, product_code)


@dataclass(frozen=True)
class CmeSpecs:
    """
    tables: raw rows for each CSV family, after normalization.
    index:  per-table map from (exchange, product_code) -> list of row indices.
    """

    root: Path
    tables: dict[TableName, list[dict[str, Any]]]
    index: dict[TableName, dict[Key, list[int]]]


# ---------------------- loader ----------------------


def load_cme_specs(root: Path) -> CmeSpecs:
    root = Path(root)

    def maybe(name: str) -> list[dict[str, Any]]:
        p = root / name
        return _read_csv(p) if p.exists() else []

    tables: dict[str, list[dict[str, Any]]] = {
        "prf": maybe("cmeg.fut.prf.csv"),
        "gpr": maybe("globex-product-reference-sheet.csv"),
        "nrr": maybe("globex-nrr.csv"),
        "spl": maybe("special-price-fluctuation-limits.csv"),
        "position_limits": [],
    }
    # merge position limits variants if present
    for fname in (
        "position-limits-cme.csv",
        "position-limits-cbot.csv",
        "position-limits-nymex-comex.csv",
    ):
        p = root / fname
        if p.exists():
            tables["position_limits"].extend(_read_csv(p))

    # build indices
    index: dict[str, dict[Key, list[int]]] = {}
    for tname, rows in tables.items():
        idx: dict[Key, list[int]] = {}
        for i, r in enumerate(rows):
            exch = str(r.get("exchange", "")).strip().upper()
            prod = str(r.get("product_code", "")).strip().upper()
            if not exch or not prod:
                # keep row, but do not index without keys
                continue
            key = (exch, prod)
            idx.setdefault(key, []).append(i)
        index[tname] = idx

    return CmeSpecs(root=root, tables=tables, index=index)


# ---------------------- generic accessors ----------------------


def list_tables(specs: CmeSpecs) -> list[TableName]:
    return sorted(specs.tables.keys())


def columns(specs: CmeSpecs, table: TableName) -> list[str]:
    rows = specs.tables.get(table, [])
    if not rows:
        return []
    # union of keys across a few rows to be safe
    keys: set[str] = set()
    for r in rows[:10]:
        keys.update(r.keys())
    return sorted(keys)


def rows(
    specs: CmeSpecs,
    table: TableName,
    product_code: str | None = None,
    exchange: str | None = None,
) -> list[dict[str, Any]]:
    data = specs.tables.get(table, [])
    if product_code is None and exchange is None:
        return list(data)
    # normalize key
    ex = exchange.strip().upper() if isinstance(exchange, str) else None
    pc = product_code.strip().upper() if isinstance(product_code, str) else None

    if ex and pc:
        idx = specs.index.get(table, {}).get((ex, pc), [])
        return [data[i] for i in idx]

    # partial filter (slower path)
    out: list[dict[str, Any]] = []
    for r in data:
        if ex and str(r.get("exchange", "")).strip().upper() != ex:
            continue
        if pc and str(r.get("product_code", "")).strip().upper() != pc:
            continue
        out.append(r)
    return out


def first(
    specs: CmeSpecs,
    table: TableName,
    product_code: str,
    exchange: str | None = None,
) -> dict[str, Any] | None:
    rs = rows(specs, table, product_code=product_code, exchange=exchange)
    return rs[0] if rs else None


def get_value(
    specs: CmeSpecs,
    table: TableName,
    product_code: str,
    column: str,
    exchange: str | None = None,
    default: Any = None,
) -> Any:
    r = first(specs, table, product_code=product_code, exchange=exchange)
    if not r:
        return default
    return r.get(column, default)


def iter_products(specs: CmeSpecs, table: TableName | None = None) -> set[Key]:
    keys: set[Key] = set()
    if table:
        keys.update(specs.index.get(table, {}).keys())
    else:
        for t in specs.index.values():
            keys.update(t.keys())
    return keys


def search(
    specs: CmeSpecs,
    table: TableName,
    where: dict[str, Any] | None = None,
    pred: Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
    """
    Simple filtering:
      - where={"matching_algorithm": "Fifo"}  (exact string match after str() + lower().strip())
      - pred=lambda r: float(r.get("price_band_points", 0) or 0) > 5
    """
    data = specs.tables.get(table, [])
    if not data:
        return []

    def _ok_dict(r: dict[str, Any]) -> bool:
        if not where:
            return True
        for k, v in where.items():
            if k not in r:
                return False
            a = str(r[k]).strip().lower()
            b = str(v).strip().lower()
            if a != b:
                return False
        return True

    out: list[dict[str, Any]] = []
    for r in data:
        if not _ok_dict(r):
            continue
        if pred is not None and not pred(r):
            continue
        out.append(r)
    return out


# ---------------------- convenience math (optional) ----------------------


def tick_size(specs: CmeSpecs, product_code: str, exchange: str | None = None) -> float | None:
    """
    Returns tick size (MinPxIncr) from PRF, if present.
    """
    r = first(specs, "prf", product_code, exchange)
    if not r:
        return None
    for k in ("minpxincr", "min_price_increment", "min_price_incr"):
        if k in r and r[k] not in ("", None):
            return _to_float(r[k])
    return None


def dollars_per_tick(
    specs: CmeSpecs, product_code: str, exchange: str | None = None
) -> float | None:
    """
    $/tick = (MinPxIncr / PriceDisplayFactor) * Mult
    """
    r = first(specs, "prf", product_code, exchange)
    if not r:
        return None

    min_tick = None
    for k in ("minpxincr", "min_price_increment", "min_price_incr"):
        if k in r and r[k] not in ("", None):
            min_tick = _to_float(r[k])
            break

    mult = None
    for k in ("mult", "contract_multiplier", "contract_mult"):
        if k in r and r[k] not in ("", None):
            mult = _to_float(r[k])
            break

    pdf = None
    for k in ("pricedisplayfactor", "pxdisplayfactor", "price_display_factor"):
        if k in r and r[k] not in ("", None):
            pdf = _to_float(r[k])
            break
    if pdf is None:
        pdf = 1.0

    if min_tick is None or mult is None:
        return None
    return (min_tick / float(pdf)) * float(mult)
