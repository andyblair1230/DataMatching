import tempfile
from pathlib import Path

import yaml
from click.testing import CliRunner

from sierra_sync.cli import cli


def test_cli_with_config_override():
    runner = CliRunner()

    # Create a temporary YAML config file with overrides
    temp_config = {
        "data_root": "/tmp/data_override",
        "logs_root": "/tmp/logs_override",
        "timezone": "America/New_York",
    }
    with tempfile.NamedTemporaryFile(delete=False, suffix=".yaml") as tmp:
        yaml.safe_dump(temp_config, tmp)
        tmp_path = Path(tmp.name)

    # Run CLI with --config pointing to temp file
    result = runner.invoke(cli, ["--config", str(tmp_path), "doctor"])

    assert result.exit_code == 0
    assert "data_override" in result.output
    assert "logs_override" in result.output
    assert "America/New_York" in result.output
