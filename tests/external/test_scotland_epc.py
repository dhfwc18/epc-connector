"""
Tests for ``epc_connector.external.scotland_epc``.
"""

import json
import re
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from epc_connector import validation
from epc_connector.external import scotland_epc
from epc_connector.external.scotland_epc import ScotlandEpcDataset
from epc_connector.ingest import IngestError
from tests.fakes import SOURCE_URL, FakeServer, abc_schema, scottish_csv, write_7z

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

    schemas_dir = tmp_path / "schemas"
    schema_path = schemas_dir / "source" / "scotland" / "domestic.schema.json"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(json.dumps(abc_schema()))

    def run(server: FakeServer) -> Path:
        with server.client() as client:
            return scotland_epc.download_scotland_epc(
                "domestic",
                data_dir=tmp_path / "data",
                staging_dir=tmp_path / ".staging",
                schemas_dir=schemas_dir,
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

    def test_data_breaking_the_schema_is_not_published(
        self, download_domestic: DownloadDomestic, tmp_path: Path
    ) -> None:
        archive = write_7z(tmp_path / "bad.7z", {"2024.csv": scottish_csv(["one,b,x"])})

        with pytest.raises(IngestError, match="A: 1 invalid"):
            download_domestic(FakeServer(archive.read_bytes()))

        assert not (tmp_path / "data").exists()
        assert not (tmp_path / ".staging").exists()

    def test_repository_schemas_exist_for_every_series(self) -> None:
        for dataset in ScotlandEpcDataset:
            schema_path = (
                scotland_epc.DEFAULT_SCHEMAS_DIR
                / "source"
                / "scotland"
                / f"{dataset}.schema.json"
            )
            assert schema_path.is_file()


@pytest.fixture(scope="module")
def domestic_rules() -> dict[str, pl.Expr]:
    """Polars rules for the repository's domestic schema."""
    path = scotland_epc.DEFAULT_SCHEMAS_DIR / "source/scotland/domestic.schema.json"
    return validation.column_rules(validation.load_schema(path))


class TestDomesticSchema:
    """Value rules of the repository schema for the domestic series."""

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("FLOOR_HEIGHT", "2.4"),
            ("FLOOR_HEIGHT", "3"),
            ("FLOOR_HEIGHT", ".30"),
            ("UNHEATED_CORRIDOR_LENGTH", "11."),
            ("SOLAR_WATER_HEATING_FLAG", "Y"),
            ("SOLAR_WATER_HEATING_FLAG", "false"),
            ("SOLAR_WATER_HEATING_FLAG", ""),
        ],
    )
    def test_published_value_is_valid(
        self, domestic_rules: dict[str, pl.Expr], column: str, value: str
    ) -> None:
        assert pl.DataFrame({column: [value]}).select(domestic_rules[column]).item()

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("FLOOR_HEIGHT", "."),
            ("FLOOR_HEIGHT", "abc"),
            ("FLOOR_HEIGHT", "1.2.3"),
            ("SOLAR_WATER_HEATING_FLAG", "TRUE"),
        ],
    )
    def test_malformed_value_is_invalid(
        self, domestic_rules: dict[str, pl.Expr], column: str, value: str
    ) -> None:
        assert not pl.DataFrame({column: [value]}).select(domestic_rules[column]).item()
