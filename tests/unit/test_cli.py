from sierra_sync import __version__
from sierra_sync.cli import main


def test_imports():
    assert isinstance(__version__, str) and __version__


def test_cli_help_runs():
    assert main([]) == 0


def test_cli_version_runs():
    assert main(["version"]) == 0
