"""
Shared pytest fixtures.
"""

from pathlib import Path

import pytest

from epc_connector import ingest
from tests.fakes import write_7z, write_vocabularies, yearly_members


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry immediately instead of backing off."""
    monkeypatch.setattr(ingest, "RETRY_BASE_DELAY_SECONDS", 0.0)


@pytest.fixture
def yearly_archive(tmp_path: Path) -> Path:
    """A 7z archive of three out-of-order yearly CSVs plus a PDF note."""
    return write_7z(tmp_path / "yearly.7z", yearly_members())


@pytest.fixture
def vocabularies(tmp_path: Path) -> Path:
    """Small walls and main-heating vocabularies (see ``fakes.write_vocabularies``)."""
    return write_vocabularies(tmp_path / "vocabularies")
