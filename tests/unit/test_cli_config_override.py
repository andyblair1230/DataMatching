import textwrap

from sierra_sync.cli import main


def test_cli_with_yaml_override(tmp_path, capsys):
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

    rc = main(["doctor", "--config", str(yml)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "C:\\yaml-data" in out
    assert "C:\\yaml-logs" in out
    assert "America/Chicago" in out


def test_cli_with_env_override(monkeypatch, capsys):
    monkeypatch.setenv("SIERRA_DATA_ROOT", r"C:\env-data")
    monkeypatch.setenv("SIERRA_LOGS_ROOT", r"C:\env-logs")
    monkeypatch.setenv("SIERRA_TIMEZONE", "UTC")

    rc = main(["doctor"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "C:\\env-data" in out
    assert "C:\\env-logs" in out
    assert "UTC" in out
