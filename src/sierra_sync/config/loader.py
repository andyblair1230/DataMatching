from __future__ import annotations

import os
import typing as t
from dataclasses import dataclass, replace
from pathlib import Path

import yaml

from .defaults import DEFAULTS, Defaults

ENV_PREFIX = "SIERRA_"


@dataclass(frozen=True)
class Config:
    data_root: Path
    logs_root: Path
    timezone: str


def _from_defaults() -> Config:
    d: Defaults = DEFAULTS
    return Config(
        data_root=Path(d.data_root),
        logs_root=Path(d.logs_root),
        timezone=d.timezone,
    )


def _apply_env(cfg: Config) -> Config:
    def env_path(key: str, cur: Path) -> Path:
        v = os.environ.get(f"{ENV_PREFIX}{key}")
        return Path(v) if v else cur

    def env_str(key: str, cur: str) -> str:
        v = os.environ.get(f"{ENV_PREFIX}{key}")
        return v if v else cur

    return replace(
        cfg,
        data_root=env_path("DATA_ROOT", cfg.data_root),
        logs_root=env_path("LOGS_ROOT", cfg.logs_root),
        timezone=env_str("TIMEZONE", cfg.timezone),
    )


def _apply_yaml(cfg: Config, path: Path | None) -> Config:
    if not path:
        return cfg
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        doc: dict[str, t.Any] = yaml.safe_load(f) or {}

    data_root = Path(doc.get("data_root", cfg.data_root))
    logs_root = Path(doc.get("logs_root", cfg.logs_root))
    timezone = doc.get("timezone", cfg.timezone)

    return replace(cfg, data_root=data_root, logs_root=logs_root, timezone=timezone)


def load_config(yaml_path: str | Path | None = None) -> Config:
    """
    Load configuration with this precedence:
    1) Defaults (code)
    2) Environment variables (prefixed SIERRA_)
    3) YAML file (explicit path)
    """
    cfg = _from_defaults()
    cfg = _apply_env(cfg)
    cfg = _apply_yaml(cfg, Path(yaml_path) if yaml_path else None)

    # Minimal validation
    if not cfg.timezone:
        raise ValueError("timezone must be set")
    return cfg
