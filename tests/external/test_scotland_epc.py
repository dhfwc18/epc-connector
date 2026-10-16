"""
Tests for ``epc_connector.external.scotland_epc``.
"""

import re
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from epc_connector.external import scotland_epc
from epc_connector.external.scotland_epc import ScotlandEpcDataset
from epc_connector.ingest import IngestError
from tests.fakes import SOURCE_URL, FakeServer, write_7z

type DownloadDomestic = Callable[[FakeServer], Path]

URL_CONSTANTS = {
    "domestic": scotland_epc.DOMESTIC_URL,
    "non-domestic": scotland_epc.NON_DOMESTIC_URL,
}


@pytest.fixture
def download_domestic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> DownloadDomestic:
    """Run ``download_scotland_epc("domestic")`` against a fake server."""
    monkeypatch.setitem(
        scotland_epc.SOURCE_URLS, ScotlandEpcDataset.DOMESTIC, SOURCE_URL
    )

    def run(server: FakeServer) -> Path:
        with server.client() as client:
            return scotland_epc.download_scotland_epc(
                "domestic",
                data_dir=tmp_path / "data",
                staging_dir=tmp_path / ".staging",
                client=client,
            )

    return run


class TestSourceConstants:
    @pytest.mark.parametrize("url", URL_CONSTANTS.values(), ids=URL_CONSTANTS.keys())
    def test_url_points_at_a_ckan_resource_download(self, url: str) -> None:
        assert re.search(r"/resource/[0-9a-f-]{36}/download$", url)

    def test_every_series_has_a_url_and_parquet_name(self) -> None:
        assert set(scotland_epc.SOURCE_URLS) == set(ScotlandEpcDataset)
        assert set(scotland_epc.PARQUET_NAMES) == set(ScotlandEpcDataset)


class TestPeriodSortKey:
    def test_quarters_and_years_sort_oldest_first(self) -> None:
        names = ["Q1 2017.csv", "Q3 2016.csv", "2008.csv", "Q4 2016.csv", "2026.csv"]

        ordered = sorted((Path(n) for n in names), key=scotland_epc.period_sort_key)

        assert [p.name for p in ordered] == [
            "2008.csv",
            "Q3 2016.csv",
            "Q4 2016.csv",
            "Q1 2017.csv",
            "2026.csv",
        ]


class TestDownloadScotlandEpc:
    def test_series_is_merged_into_named_parquet_file(
        self,
        download_domestic: DownloadDomestic,
        quarterly_archive: Path,
        tmp_path: Path,
    ) -> None:
        output = download_domestic(FakeServer(quarterly_archive.read_bytes()))

        assert output == tmp_path / "data" / "domestic_epc_certificate_scotland.parquet"
        assert pl.read_parquet(output)["A"].to_list() == ["1", "2", "3", "4"]

    def test_staging_directory_is_removed_after_success(
        self,
        download_domestic: DownloadDomestic,
        quarterly_archive: Path,
        tmp_path: Path,
    ) -> None:
        download_domestic(FakeServer(quarterly_archive.read_bytes()))

        assert not (tmp_path / ".staging").exists()

    def test_existing_parquet_file_is_replaced(
        self, download_domestic: DownloadDomestic, quarterly_archive: Path
    ) -> None:
        server = FakeServer(quarterly_archive.read_bytes())
        output = download_domestic(server)
        output.write_bytes(b"stale")

        download_domestic(server)

        assert pl.read_parquet(output).height == 4

    def test_staging_directory_is_removed_after_failure(
        self, download_domestic: DownloadDomestic, tmp_path: Path
    ) -> None:
        archive = write_7z(tmp_path / "no_csv.7z", {"note.pdf": b"%PDF"})

        with pytest.raises(IngestError):
            download_domestic(FakeServer(archive.read_bytes()))

        assert not (tmp_path / ".staging").exists()
        assert not (tmp_path / "data").exists()

    def test_unknown_series_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="not-a-series"):
            scotland_epc.download_scotland_epc("not-a-series")
