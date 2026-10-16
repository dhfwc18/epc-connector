"""
Shared pytest fixtures.
"""

from pathlib import Path

import pytest

from epc_connector import ingest
from tests.fakes import quarterly_members, write_7z


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry immediately instead of backing off."""
    monkeypatch.setattr(ingest, "RETRY_BASE_DELAY_SECONDS", 0.0)


@pytest.fixture
def quarterly_archive(tmp_path: Path) -> Path:
    """A 7z archive of three out-of-order quarterly CSVs plus a PDF note."""
    return write_7z(tmp_path / "quarterly.7z", quarterly_members())
