import textwrap
from pathlib import Path

from sierra_sync.config.loader import DEFAULTS, load_config


def test_defaults_load():
    cfg = load_config()
    assert cfg.data_root == Path(DEFAULTS.data_root)
    assert cfg.logs_root == Path(DEFAULTS.logs_root)
    assert isinstance(cfg.timezone, str) and cfg.timezone


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("SIERRA_DATA_ROOT", r"C:\override-data")
    monkeypatch.setenv("SIERRA_LOGS_ROOT", r"C:\override-logs")
    monkeypatch.setenv("SIERRA_TIMEZONE", "UTC")
    cfg = load_config()
    assert str(cfg.data_root).lower().endswith(r"\override-data")
    assert str(cfg.logs_root).lower().endswith(r"\override-logs")
    assert cfg.timezone == "UTC"


def test_yaml_overrides(tmp_path, monkeypatch):
    # set env to something, then ensure YAML beats it
    monkeypatch.setenv("SIERRA_TIMEZONE", "EnvTZ")
    yml = tmp_path / "settings.yaml"
    yml.write_text(
        textwrap.dedent(
            """
        data_root: C:\\yaml-data
        logs_root: C:\\yaml-logs
        timezone: America/Chicago
    """
        ).strip(),
        encoding="utf-8",
    )
    cfg = load_config(yml)
    assert str(cfg.data_root).lower().endswith(r"\yaml-data")
    assert str(cfg.logs_root).lower().endswith(r"\yaml-logs")
    assert cfg.timezone == "America/Chicago"
