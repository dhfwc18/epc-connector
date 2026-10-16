"""
Tests for ``epc_connector.external.england_wales_epc``.
"""

import functools
import json
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest

from epc_connector import validation
from epc_connector.external import england_wales_epc
from epc_connector.external.england_wales_epc import EnglandWalesEpcDataset
from epc_connector.ingest import IngestError
from tests.fakes import FakeServer, write_zip

TOKEN = "test-token"
URL_CONSTANTS = {
    "domestic": england_wales_epc.DOMESTIC_URL,
    "non-domestic": england_wales_epc.NON_DOMESTIC_URL,
    "display": england_wales_epc.DISPLAY_URL,
}


def certificates_csv(rows: list[str]) -> bytes:
    """Return a CSV shaped like the bulk export: lowercase header, no label row."""
    return (
        "certificate_number,lodgement_date,value\n" + "\n".join(rows) + "\n"
    ).encode()


def certificates_schema() -> dict[str, Any]:
    """Return a source schema for ``certificates_csv`` files."""
    date = {"pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "$defs": {"missing": {"enum": [""]}},
        "properties": {
            "certificate_number": {"type": "string"},
            "lodgement_date": {
                "type": "string",
                "anyOf": [{"$ref": "#/$defs/missing"}, date],
            },
            "value": {"type": "string"},
        },
        "required": ["certificate_number", "lodgement_date", "value"],
        "additionalProperties": False,
    }


class SeriesRunner:
    """Run the domestic series against a fake API inside ``tmp_path``."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.data_dir = tmp_path / "data"
        self.staging_dir = tmp_path / ".staging"
        self.schemas_dir = tmp_path / "schemas"
        path = (
            self.schemas_dir / "source" / "england-and-wales" / "domestic.schema.json"
        )
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(certificates_schema()))

    @property
    def parquet_path(self) -> Path:
        return self.data_dir / "domestic_epc_certificate_england_wales.parquet"

    def server(self, members: dict[str, bytes]) -> FakeServer:
        archive = write_zip(self.tmp_path / "source.zip", members)
        return FakeServer(
            archive.read_bytes(), source_url=england_wales_epc.DOMESTIC_URL, token=TOKEN
        )

    def download(self, server: FakeServer, token: str = TOKEN) -> Path:
        client = england_wales_epc.authorised_client(
            token, transport=server.transport()
        )
        with client:
            return england_wales_epc.download_england_and_wales_epc(
                "domestic",
                data_dir=self.data_dir,
                staging_dir=self.staging_dir,
                schemas_dir=self.schemas_dir,
                client=client,
            )


@pytest.fixture
def runner(tmp_path: Path) -> SeriesRunner:
    return SeriesRunner(tmp_path)


def two_years_with_recommendations() -> dict[str, bytes]:
    """Return a bulk ZIP's members, out of year order, with a recommendations file."""
    return {
        "certificates-2024.csv": certificates_csv(
            ["c3,2024-02-01,x", "c4,2024-09-30,"]
        ),
        "certificates-2023.csv": certificates_csv(
            ["c1,2023-01-15,y", "c2,2023-12-31,z"]
        ),
        "recommendations-2024.csv": b"certificate_number,improvement_item\nc3,1\n",
    }


class TestSourceConstants:
    @pytest.mark.parametrize("url", URL_CONSTANTS.values(), ids=URL_CONSTANTS.keys())
    def test_url_points_at_a_bulk_csv_endpoint(self, url: str) -> None:
        assert url.startswith(f"{england_wales_epc.API_URL}/api/files/")
        assert url.endswith("/csv")

    def test_every_series_has_a_url_and_parquet_name(self) -> None:
        assert set(england_wales_epc.SOURCE_URLS) == set(EnglandWalesEpcDataset)
        assert set(england_wales_epc.PARQUET_NAMES) == set(EnglandWalesEpcDataset)

    def test_repository_schemas_exist_for_every_series(self) -> None:
        for dataset in EnglandWalesEpcDataset:
            schema_path = (
                england_wales_epc.DEFAULT_SCHEMAS_DIR
                / "source"
                / "england-and-wales"
                / f"{dataset}.schema.json"
            )
            assert schema_path.is_file()


@pytest.fixture
def token_unset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """
    Remove ``EPB_API_TOKEN`` and work in an empty directory (no ``.env``).

    Setting then deleting registers the variable with monkeypatch, so a value
    written by ``load_dotenv`` is removed again after the test.
    """
    monkeypatch.setenv(england_wales_epc.API_TOKEN_ENV_VAR, "placeholder")
    monkeypatch.delenv(england_wales_epc.API_TOKEN_ENV_VAR)
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestAuthorisedClient:
    def test_token_argument_sets_bearer_header(self) -> None:
        with england_wales_epc.authorised_client("abc") as client:
            assert client.headers["Authorization"] == "Bearer abc"

    def test_environment_variable_is_used_without_argument(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(england_wales_epc.API_TOKEN_ENV_VAR, "from-env")

        with england_wales_epc.authorised_client() as client:
            assert client.headers["Authorization"] == "Bearer from-env"

    def test_dotenv_file_is_loaded_when_variable_is_unset(
        self, token_unset: Path
    ) -> None:
        (token_unset / ".env").write_text("EPB_API_TOKEN=from-dotenv\n")

        with england_wales_epc.authorised_client() as client:
            assert client.headers["Authorization"] == "Bearer from-dotenv"

    def test_environment_variable_takes_precedence_over_dotenv(
        self, token_unset: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (token_unset / ".env").write_text("EPB_API_TOKEN=from-dotenv\n")
        monkeypatch.setenv(england_wales_epc.API_TOKEN_ENV_VAR, "from-env")

        with england_wales_epc.authorised_client() as client:
            assert client.headers["Authorization"] == "Bearer from-env"

    def test_missing_token_raises_value_error(self, token_unset: Path) -> None:
        with pytest.raises(ValueError, match="EPB_API_TOKEN"):
            england_wales_epc.authorised_client()


class TestIsCertificatesFile:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("certificates-2024.csv", True),
            ("folder/certificates-2008.csv", True),
            ("recommendations-2024.csv", False),
            ("certificates.csv", False),
        ],
    )
    def test_only_yearly_certificate_files_match(
        self, name: str, expected: bool
    ) -> None:
        assert england_wales_epc.is_certificates_file(name) is expected


class TestYearSortKey:
    def test_years_sort_oldest_first(self) -> None:
        names = [
            "certificates-2024.csv",
            "certificates-2008.csv",
            "certificates-2012.csv",
        ]

        ordered = sorted((Path(n) for n in names), key=england_wales_epc.year_sort_key)

        assert [p.name for p in ordered] == [
            "certificates-2008.csv",
            "certificates-2012.csv",
            "certificates-2024.csv",
        ]


class TestDownloadEnglandAndWalesEpc:
    def test_certificates_are_merged_oldest_first_without_recommendations(
        self, runner: SeriesRunner
    ) -> None:
        output = runner.download(runner.server(two_years_with_recommendations()))

        frame = pl.read_parquet(output)
        assert output == runner.parquet_path
        assert frame.columns == ["certificate_number", "lodgement_date", "value"]
        assert frame["certificate_number"].to_list() == ["c1", "c2", "c3", "c4"]
        assert frame["value"].to_list() == ["y", "z", "x", ""]

    def test_last_updated_is_stored_in_parquet_metadata(
        self, runner: SeriesRunner
    ) -> None:
        server = runner.server(two_years_with_recommendations())

        output = runner.download(server)

        stored = pl.read_parquet_metadata(output)
        assert (
            stored[england_wales_epc.LAST_UPDATED_METADATA_KEY] == server.last_updated
        )

    def test_token_is_not_sent_to_the_presigned_url(self, runner: SeriesRunner) -> None:
        server = runner.server(two_years_with_recommendations())

        runner.download(server)

        assert server.calls["file"] > 0
        assert server.calls["file_with_authorization"] == 0

    def test_wrong_token_raises_http_error_and_removes_staging(
        self, runner: SeriesRunner
    ) -> None:
        server = runner.server(two_years_with_recommendations())

        with pytest.raises(httpx.HTTPStatusError, match="401"):
            runner.download(server, token="wrong-token")

        assert not runner.staging_dir.exists()
        assert not runner.data_dir.exists()

    def test_data_breaking_the_schema_is_not_published(
        self, runner: SeriesRunner
    ) -> None:
        members = {"certificates-2024.csv": certificates_csv(["c1,01/02/2024,x"])}

        with pytest.raises(IngestError, match="lodgement_date: 1 invalid"):
            runner.download(runner.server(members))

        assert not runner.data_dir.exists()
        assert not runner.staging_dir.exists()

    def test_unknown_series_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="not-a-series"):
            england_wales_epc.download_england_and_wales_epc("not-a-series")


@functools.cache
def repository_rules(series: str) -> dict[str, pl.Expr]:
    """Polars rules for the repository's schema of an England and Wales series."""
    path = (
        england_wales_epc.DEFAULT_SCHEMAS_DIR
        / "source"
        / "england-and-wales"
        / f"{series}.schema.json"
    )
    return validation.column_rules(validation.load_schema(path))


def is_valid(series: str, column: str, value: str) -> bool:
    """True if ``value`` passes the repository schema's rule for ``column``."""
    rule = repository_rules(series)[column]
    return pl.DataFrame({column: [value]}).select(rule).item()


class TestDisplaySchema:
    """Value rules of the repository schema for the display series."""

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("main_benchmark", "University campus"),
            ("main_benchmark", "Schools And Seasonal Public Buildings"),
            ("nominated_date", "2019-04-30"),
            ("nominated_date", "Dec-2008"),
            ("nominated_date", ""),
        ],
    )
    def test_published_value_is_valid(self, column: str, value: str) -> None:
        assert is_valid("display", column, value)

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("nominated_date", "2008-12"),
            ("nominated_date", "December-2008"),
            ("nominated_date", "dec-2008"),
        ],
    )
    def test_malformed_value_is_invalid(self, column: str, value: str) -> None:
        assert not is_valid("display", column, value)


class TestDomesticSchema:
    """Value rules of the repository schema for the domestic series."""

    @pytest.mark.parametrize("value", ["100", "100.0", "50", ""])
    def test_published_multi_glaze_proportion_is_valid(self, value: str) -> None:
        assert is_valid("domestic", "multi_glaze_proportion", value)

    @pytest.mark.parametrize("value", ["abc", "1.2.3", "."])
    def test_malformed_multi_glaze_proportion_is_invalid(self, value: str) -> None:
        assert not is_valid("domestic", "multi_glaze_proportion", value)
