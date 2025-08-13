from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .defaults import DEFAULTS


@dataclass(frozen=True)
class Config:
    scid_root: Path
    depth_root: Path
    logs_root: Path
    timezone: str
    refdata_file: Path | None
    cme_specs_root: Path | None


def _req_path(base: dict[str, Any], key: str, default: str) -> Path:
    """Return a required Path, falling back to default if missing/empty."""
    val = base.get(key) or default
    return Path(val)


def _opt_path(base: dict[str, Any], key: str) -> Path | None:
    """Return an optional Path, or None if missing/empty."""
    val = base.get(key)
    return Path(val) if val else None


def _apply_yaml_overrides(base: dict[str, Any], yml: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k in ("scid_root", "depth_root", "logs_root", "timezone", "refdata_file", "cme_specs_root"):
        if k in yml:
            out[k] = yml[k]
    return out


def _apply_env_overrides(base: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    if v := os.getenv("SIERRA_SCID_ROOT"):
        out["scid_root"] = v
    if v := os.getenv("SIERRA_DEPTH_ROOT"):
        out["depth_root"] = v
    if v := os.getenv("SIERRA_LOGS_ROOT"):
        out["logs_root"] = v
    if v := os.getenv("SIERRA_TIMEZONE"):
        out["timezone"] = v
    if v := os.getenv("SIERRA_REFDATA_FILE"):
        out["refdata_file"] = v
    if v := os.getenv("SIERRA_CME_SPECS_ROOT"):
        out["cme_specs_root"] = v
    return out


def load_config(yaml_path: Path | None = None) -> Config:
    # start from defaults as a dict
    base = {
        "scid_root": DEFAULTS.scid_root,
        "depth_root": DEFAULTS.depth_root,
        "logs_root": DEFAULTS.logs_root,
        "timezone": DEFAULTS.timezone,
        "refdata_file": DEFAULTS.refdata_file,
        "cme_specs_root": DEFAULTS.cme_specs_root,
    }

    # ENV overrides (middle precedence)
    base = _apply_env_overrides(base)

    # YAML overrides (highest precedence)
    if yaml_path:
        data = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Config YAML must be a mapping at the top level.")
        base = _apply_yaml_overrides(base, data)

    return Config(
        scid_root=_req_path(base, "scid_root", DEFAULTS.scid_root),
        depth_root=_req_path(base, "depth_root", DEFAULTS.depth_root),
        logs_root=_req_path(base, "logs_root", DEFAULTS.logs_root),
        timezone=str(base["timezone"]),
        refdata_file=_opt_path(base, "refdata_file"),
        cme_specs_root=_opt_path(base, "cme_specs_root"),
    )
