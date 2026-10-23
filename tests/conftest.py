"""
Shared pytest fixtures.
"""

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from epc_connector.cli import logs
from epc_connector.environment import EnvVar
from epc_connector.io import downloads
from tests.fakes import write_7z, write_vocabularies, yearly_members


@pytest.fixture(autouse=True)
def default_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point the default data directory at ``tmp_path``, never the project."""
    path = tmp_path / "default-data"
    monkeypatch.setenv(EnvVar.DATA_DIR, str(path))
    return path


@pytest.fixture(autouse=True)
def saved_token_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """
    Hide the user's API token: unset its variable and save to tmp_path.

    Returns where the command line would save a token.
    """
    monkeypatch.delenv(EnvVar.API_TOKEN, raising=False)
    path = tmp_path / "config" / "token"
    monkeypatch.setenv(EnvVar.TOKEN_PATH, str(path))
    return path


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry immediately instead of backing off."""
    monkeypatch.setattr(downloads, "RETRY_BASE_DELAY_SECONDS", 0.0)


@pytest.fixture
def yearly_archive(tmp_path: Path) -> Path:
    """A 7z archive of three out-of-order yearly CSVs plus a PDF note."""
    return write_7z(tmp_path / "yearly.7z", yearly_members())


@pytest.fixture
def vocabularies(tmp_path: Path) -> Path:
    """Small walls and main-heating vocabularies (see ``fakes``)."""
    return write_vocabularies(tmp_path / "vocabularies")


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Undo ``cli.logs.configure_logging`` after the test."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)
    for name in logs.QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.NOTSET)
