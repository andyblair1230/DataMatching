import textwrap
from pathlib import Path

from sierra_sync.config.loader import DEFAULTS, load_config


def test_defaults_load():
    cfg = load_config()
    assert cfg.scid_root == Path(DEFAULTS.scid_root)
    assert cfg.depth_root == Path(DEFAULTS.depth_root)
    assert cfg.logs_root == Path(DEFAULTS.logs_root)
    assert isinstance(cfg.timezone, str) and cfg.timezone


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("SIERRA_SCID_ROOT", r"C:\override-scid")
    monkeypatch.setenv("SIERRA_DEPTH_ROOT", r"C:\override-depth")
    monkeypatch.setenv("SIERRA_LOGS_ROOT", r"C:\override-logs")
    monkeypatch.setenv("SIERRA_TIMEZONE", "UTC")
    cfg = load_config()
    assert str(cfg.scid_root).lower().endswith(r"\override-scid")
    assert str(cfg.depth_root).lower().endswith(r"\override-depth")
    assert str(cfg.logs_root).lower().endswith(r"\override-logs")
    assert cfg.timezone == "UTC"


def test_yaml_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("SIERRA_TIMEZONE", "EnvTZ")
    yml = tmp_path / "settings.yaml"
    yml.write_text(
        textwrap.dedent(
            """
        scid_root: C:\\yaml-scid
        depth_root: C:\\yaml-depth
        logs_root: C:\\yaml-logs
        timezone: America/Chicago
    """
        ).strip(),
        encoding="utf-8",
    )
    cfg = load_config(yml)
    assert str(cfg.scid_root).lower().endswith(r"\yaml-scid")
    assert str(cfg.depth_root).lower().endswith(r"\yaml-depth")
    assert str(cfg.logs_root).lower().endswith(r"\yaml-logs")
    assert cfg.timezone == "America/Chicago"
