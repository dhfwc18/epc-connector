"""
Tests for ``epc_connector.sources.england_and_wales``.
"""

import datetime as dt
import functools
import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import pytest

from epc_connector import validation
from epc_connector.credentials import ApiTokenError
from epc_connector.datasets import DatasetError, Origin, Segment, select_datasets
from epc_connector.environment import EnvVar
from epc_connector.io import StorageError
from epc_connector.sources import england_and_wales
from tests.fakes import FakeServer, write_zip

TOKEN = "test-token"
URL_CONSTANTS = {
    "domestic": england_and_wales.DOMESTIC_URL,
    "non-domestic": england_and_wales.NON_DOMESTIC_URL,
    "display": england_and_wales.DISPLAY_URL,
}


def certificates_csv(rows: list[str]) -> bytes:
    """
    Return a CSV shaped like the bulk export: lowercase header, no label row.

    Each ``"number,date,value"`` row gets UPRN 1 from an energy assessor.
    """
    header = "certificate_number,lodgement_date,value,uprn,uprn_source"
    lines = [f"{row},1,Energy Assessor" for row in rows]
    return ("\n".join([header, *lines]) + "\n").encode()


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
            "uprn": {"type": "string"},
            "uprn_source": {"type": "string"},
        },
        "required": [
            "certificate_number",
            "lodgement_date",
            "value",
            "uprn",
            "uprn_source",
        ],
        "additionalProperties": False,
    }


class SegmentRunner:
    """Run the domestic segment against a fake API inside ``tmp_path``."""

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

    def server(self, members: dict[str, bytes], **options: Any) -> FakeServer:
        archive = write_zip(self.tmp_path / "source.zip", members)
        return FakeServer(
            archive.read_bytes(),
            source_url=england_and_wales.DOMESTIC_URL,
            token=TOKEN,
            **options,
        )

    def download(self, server: FakeServer, token: str = TOKEN) -> Path:
        client = england_and_wales.authorised_client(
            token, transport=server.transport()
        )
        with client:
            return england_and_wales.download(
                "domestic",
                data_dir=self.data_dir,
                staging_dir=self.staging_dir,
                schemas_dir=self.schemas_dir,
                client=client,
            )

    def update(
        self, server: FakeServer, date_from: str | None, date_to: str | None
    ) -> Path:
        client = england_and_wales.authorised_client(
            TOKEN, transport=server.transport()
        )
        with client:
            return england_and_wales.update(
                "domestic",
                date_from=date_from,
                date_to=date_to,
                data_dir=self.data_dir,
                staging_dir=self.staging_dir,
                schemas_dir=self.schemas_dir,
                client=client,
            )


@pytest.fixture
def runner(tmp_path: Path) -> SegmentRunner:
    return SegmentRunner(tmp_path)


def two_years_with_recommendations() -> dict[str, bytes]:
    """Return bulk ZIP members out of year order, plus recommendations."""
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
        assert url.startswith(f"{england_and_wales.API_URL}/api/files/")
        assert url.endswith("/csv")

    def test_every_published_segment_has_a_url(self) -> None:
        published = {d.segment for d in select_datasets([Origin.ENGLAND_AND_WALES])}

        assert set(england_and_wales.SOURCE_URLS) == published


class TestAuthorisedClient:
    def test_token_argument_sets_bearer_header(self) -> None:
        with england_and_wales.authorised_client("abc") as client:
            assert client.headers["Authorization"] == "Bearer abc"

    def test_environment_variable_is_used_without_argument(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EnvVar.API_TOKEN, "from-env")

        with england_and_wales.authorised_client() as client:
            assert client.headers["Authorization"] == "Bearer from-env"

    def test_missing_token_raises_api_token_error(self) -> None:
        with pytest.raises(ApiTokenError, match="epc-connector login"):
            england_and_wales.authorised_client()


def info_server(status: int) -> httpx.Client:
    """A client whose every request gets ``status`` and an info body."""

    def answer(request: httpx.Request) -> httpx.Response:
        body = {"data": {"lastUpdated": "2026-10-13T09:29:37+00:00"}}
        return httpx.Response(status, json=body, request=request)

    return httpx.Client(transport=httpx.MockTransport(answer))


class TestVerifyApiToken:
    def test_accepted_token_passes(self) -> None:
        with info_server(200) as client:
            england_and_wales.verify_api_token("good", client)

    @pytest.mark.parametrize("status", [401, 403])
    def test_rejected_token_raises_api_token_error(self, status: int) -> None:
        with info_server(status) as client, pytest.raises(ApiTokenError):
            england_and_wales.verify_api_token("bad", client)

    def test_other_errors_are_raised_as_http_errors(self) -> None:
        with info_server(500) as client, pytest.raises(httpx.HTTPStatusError):
            england_and_wales.verify_api_token("good", client)


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
        assert england_and_wales.is_certificates_file(name) is expected


class TestYearSortKey:
    def test_years_sort_oldest_first(self) -> None:
        names = [
            "certificates-2024.csv",
            "certificates-2008.csv",
            "certificates-2012.csv",
        ]

        ordered = sorted((Path(n) for n in names), key=england_and_wales.year_sort_key)

        assert [p.name for p in ordered] == [
            "certificates-2008.csv",
            "certificates-2012.csv",
            "certificates-2024.csv",
        ]


class TestDownload:
    def test_certificates_are_merged_oldest_first_without_recommendations(
        self, runner: SegmentRunner
    ) -> None:
        output = runner.download(runner.server(two_years_with_recommendations()))

        frame = pl.read_parquet(output)
        assert output == runner.parquet_path
        assert frame.columns == [
            "certificate_number",
            "lodgement_date",
            "value",
            "uprn",
            "uprn_source",
        ]
        assert frame["certificate_number"].to_list() == ["c1", "c2", "c3", "c4"]
        assert frame["value"].to_list() == ["y", "z", "x", ""]

    def test_last_updated_is_stored_in_parquet_metadata(
        self, runner: SegmentRunner
    ) -> None:
        server = runner.server(two_years_with_recommendations())

        output = runner.download(server)

        stored = pl.read_parquet_metadata(output)
        assert (
            stored[england_and_wales.LAST_UPDATED_METADATA_KEY] == server.last_updated
        )

    def test_token_is_not_sent_to_the_presigned_url(
        self, runner: SegmentRunner
    ) -> None:
        server = runner.server(two_years_with_recommendations())

        runner.download(server)

        assert server.calls["file"] > 0
        assert server.calls["file_with_authorization"] == 0

    def test_wrong_token_raises_http_error_and_removes_staging(
        self, runner: SegmentRunner
    ) -> None:
        server = runner.server(two_years_with_recommendations())

        with pytest.raises(httpx.HTTPStatusError, match="401"):
            runner.download(server, token="wrong-token")

        assert not runner.staging_dir.exists()
        assert not runner.parquet_path.exists()

    def test_data_breaking_the_schema_is_not_published(
        self, runner: SegmentRunner
    ) -> None:
        members = {"certificates-2024.csv": certificates_csv(["c1,01/02/2024,x"])}

        with pytest.raises(StorageError, match="lodgement_date: 1 invalid"):
            runner.download(runner.server(members))

        assert not runner.parquet_path.exists()
        assert not runner.staging_dir.exists()

    def test_unknown_segment_raises_dataset_error(self) -> None:
        with pytest.raises(DatasetError, match="not-a-segment"):
            england_and_wales.download("not-a-segment")


@functools.cache
def repository_rules(segment: str) -> dict[str, pl.Expr]:
    """Polars rules from the repository schema of an E&W segment."""
    path = (
        validation.DEFAULT_SCHEMAS_DIR
        / "source"
        / "england-and-wales"
        / f"{segment}.schema.json"
    )
    return validation.column_rules(validation.load_schema(path))


def is_valid(segment: str, column: str, value: str) -> bool:
    """True if ``value`` passes the repository schema's rule for ``column``."""
    rule = repository_rules(segment)[column]
    return pl.DataFrame({column: [value]}).select(rule).item()


class TestDisplaySchema:
    """Value rules of the repository schema for the display segment."""

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
    """Value rules of the repository schema for the domestic segment."""

    @pytest.mark.parametrize("value", ["100", "100.0", "50", ""])
    def test_published_multi_glaze_proportion_is_valid(self, value: str) -> None:
        assert is_valid("domestic", "multi_glaze_proportion", value)

    @pytest.mark.parametrize("value", ["abc", "1.2.3", "."])
    def test_malformed_multi_glaze_proportion_is_invalid(self, value: str) -> None:
        assert not is_valid("domestic", "multi_glaze_proportion", value)


def values_by_certificate(parquet_path: Path) -> dict[str, str]:
    frame = pl.read_parquet(parquet_path)
    return dict(zip(frame["certificate_number"], frame["value"], strict=True))


class TestSearchCertificates:
    def test_all_pages_are_collected(self) -> None:
        lodged = [(f"c{i}", "2024-05-0" + str(i)) for i in range(1, 6)]
        server = FakeServer(b"", token=TOKEN, lodged=lodged)
        client = england_and_wales.authorised_client(
            TOKEN, transport=server.transport()
        )

        with client:
            numbers = england_and_wales.search_certificates(
                Segment.DOMESTIC,
                dt.date(2024, 5, 1),
                dt.date(2024, 5, 31),
                client,
                page_size=2,
            )

        assert numbers == ["c1", "c2", "c3", "c4", "c5"]
        assert server.calls["search_page"] == 3

    def test_no_results_returns_empty_list(self) -> None:
        server = FakeServer(b"", token=TOKEN)
        client = england_and_wales.authorised_client(
            TOKEN, transport=server.transport()
        )

        with client:
            numbers = england_and_wales.search_certificates(
                Segment.DOMESTIC,
                dt.date(2024, 5, 1),
                dt.date(2024, 5, 31),
                client,
            )

        assert numbers == []

    def test_rate_limited_request_is_retried(self) -> None:
        server = FakeServer(b"", token=TOKEN, lodged=[("c1", "2024-05-01")])
        responses = iter([httpx.Response(429, headers={"retry-after": "0"})])

        def rate_limited_once(request: httpx.Request) -> httpx.Response:
            return next(responses, None) or server(request)

        client = england_and_wales.authorised_client(
            TOKEN, transport=httpx.MockTransport(rate_limited_once)
        )
        with client:
            numbers = england_and_wales.search_certificates(
                Segment.DOMESTIC,
                dt.date(2024, 5, 1),
                dt.date(2024, 5, 31),
                client,
            )

        assert numbers == ["c1"]


class TestFetchDeltas:
    def test_events_are_split_into_removed_and_uprn_updated(self) -> None:
        events = [
            {"certificateNumber": "c1", "eventType": "removed", "timestamp": "t"},
            {"certificateNumber": "c2", "eventType": "uprn_updated", "timestamp": "t"},
            {"certificateNumber": "c1", "eventType": "uprn_updated", "timestamp": "t"},
        ]
        server = FakeServer(b"", token=TOKEN, delta_events=events)
        client = england_and_wales.authorised_client(
            TOKEN, transport=server.transport()
        )

        with client:
            removed, uprn_updated = england_and_wales.fetch_deltas(
                dt.date(2024, 5, 1), dt.date(2024, 5, 31), client
            )

        assert removed == {"c1"}
        assert uprn_updated == {"c2"}

    def test_no_events_returns_empty_sets(self) -> None:
        server = FakeServer(b"", token=TOKEN)
        client = england_and_wales.authorised_client(
            TOKEN, transport=server.transport()
        )

        with client:
            result = england_and_wales.fetch_deltas(
                dt.date(2024, 5, 1), dt.date(2024, 5, 31), client
            )

        assert result == (set(), set())


class TestFetchCertificateUprn:
    @staticmethod
    def fetch(documents: dict[str, dict[str, object]]) -> tuple[str, str] | None:
        server = FakeServer(b"", token=TOKEN, documents=documents)
        client = england_and_wales.authorised_client(
            TOKEN, transport=server.transport()
        )
        with client:
            return england_and_wales.fetch_certificate_uprn("c1", client)

    def test_uprn_is_returned_as_written_in_the_bulk_csv(self) -> None:
        documents = {"c1": {"uprn": 100121241798, "uprn_source": "Energy Assessor"}}

        assert self.fetch(documents) == ("100121241798", "Energy Assessor")

    def test_missing_uprn_is_an_empty_string(self) -> None:
        assert self.fetch({"c1": {"uprn": None, "uprn_source": ""}}) == ("", "")

    def test_unpublished_certificate_returns_none(self) -> None:
        assert self.fetch({}) is None


class TestLatestLodgementDate:
    def test_future_dates_are_ignored_as_entry_errors(self, tmp_path: Path) -> None:
        path = tmp_path / "segment.parquet"
        dates = ["2024-01-15", "2108-02-01", "2024-09-30", ""]
        pl.DataFrame({"lodgement_date": dates}).write_parquet(path)

        latest = england_and_wales.latest_lodgement_date(
            path, today=dt.date(2026, 10, 16)
        )

        assert latest == dt.date(2024, 9, 30)


class TestUpdate:
    @pytest.fixture
    def downloaded(self, runner: SegmentRunner) -> SegmentRunner:
        """Local domestic file: c1, c2 (2023) and c3, c4 (2024), all "old"."""
        members = {
            "certificates-2023.csv": certificates_csv(
                ["c1,2023-03-01,old", "c2,2023-06-01,old"]
            ),
            "certificates-2024.csv": certificates_csv(
                ["c3,2024-02-01,old", "c4,2024-09-30,old"]
            ),
        }
        runner.download(runner.server(members))
        return runner

    @staticmethod
    def next_month(runner: SegmentRunner, **options: Any) -> FakeServer:
        """
        Bulk file and APIs one month later.

        c4 re-published, c5 new, c9 found by search but not yet in the bulk
        file, c1 removed and c2's UPRN changed to 2. 2010 is a large year that
        an update for 2024 must not download.
        """
        members = {
            "certificates-2010.csv": certificates_csv(
                [f"f{i},2010-01-01,{os.urandom(8).hex()}" for i in range(20_000)]
            ),
            "certificates-2023.csv": certificates_csv(
                ["c1,2023-03-01,new", "c2,2023-06-01,new"]
            ),
            "certificates-2024.csv": certificates_csv(
                ["c3,2024-02-01,new", "c4,2024-09-30,new", "c5,2024-10-05,new"]
            ),
        }
        lodged = [("c4", "2024-09-30"), ("c5", "2024-10-05"), ("c9", "2024-10-06")]
        events = [
            {"certificateNumber": "c1", "eventType": "removed", "timestamp": "t"},
            {"certificateNumber": "c2", "eventType": "uprn_updated", "timestamp": "t"},
        ]
        return runner.server(
            members,
            lodged=lodged,
            delta_events=events,
            documents={"c2": {"uprn": 2, "uprn_source": "Address Matched"}},
            last_updated="2026-11-01T00:00:00.000+00:00",
            **options,
        )

    def test_new_and_changed_certificates_replace_their_old_rows(
        self, downloaded: SegmentRunner
    ) -> None:
        downloaded.update(self.next_month(downloaded), "2024-09-01", "2024-10-31")

        assert values_by_certificate(downloaded.parquet_path) == {
            "c2": "old",  # UPRN updated: 2023 is not downloaded
            "c3": "old",  # outside the window and unchanged: kept
            "c4": "new",  # re-published in the window: replaced, not duplicated
            "c5": "new",  # new in the window: added
        }

    def test_changed_uprn_is_patched_from_the_certificate_api(
        self, downloaded: SegmentRunner
    ) -> None:
        server = self.next_month(downloaded)

        downloaded.update(server, "2024-09-01", "2024-10-31")

        frame = pl.read_parquet(downloaded.parquet_path)
        uprns = {
            row["certificate_number"]: (row["uprn"], row["uprn_source"])
            for row in frame.iter_rows(named=True)
        }
        assert uprns["c2"] == ("2", "Address Matched")
        assert uprns["c3"] == ("1", "Energy Assessor")
        assert server.calls["certificate"] == 1

    def test_uprn_change_alone_skips_the_bulk_file(
        self, downloaded: SegmentRunner
    ) -> None:
        before = pl.read_parquet_metadata(downloaded.parquet_path)
        events = [
            {"certificateNumber": "c2", "eventType": "uprn_updated", "timestamp": "t"}
        ]
        server = downloaded.server(
            {"certificates-2024.csv": certificates_csv([])},
            delta_events=events,
            documents={"c2": {"uprn": 2, "uprn_source": "Address Matched"}},
        )

        downloaded.update(server, "2024-09-01", "2024-10-31")

        frame = pl.read_parquet(downloaded.parquet_path)
        assert frame.filter(pl.col("certificate_number") == "c2")["uprn"].item() == "2"
        assert server.calls["file"] == 0
        assert pl.read_parquet_metadata(downloaded.parquet_path) == before

    def test_last_updated_metadata_is_refreshed(
        self, downloaded: SegmentRunner
    ) -> None:
        server = self.next_month(downloaded)

        downloaded.update(server, "2024-09-01", "2024-10-31")

        stored = pl.read_parquet_metadata(downloaded.parquet_path)
        assert (
            stored[england_and_wales.LAST_UPDATED_METADATA_KEY] == server.last_updated
        )

    def test_only_needed_years_are_downloaded(self, downloaded: SegmentRunner) -> None:
        server = self.next_month(downloaded)

        downloaded.update(server, "2024-09-01", "2024-10-31")

        assert server.bytes_served < len(server.payload) // 2

    def test_pending_certificates_are_logged(
        self, downloaded: SegmentRunner, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            downloaded.update(self.next_month(downloaded), "2024-09-01", "2024-10-31")

        assert "1 pending" in caplog.text

    def test_nothing_new_skips_the_bulk_file(self, downloaded: SegmentRunner) -> None:
        before = downloaded.parquet_path.read_bytes()
        server = downloaded.server({"certificates-2024.csv": certificates_csv([])})

        downloaded.update(server, "2024-09-01", "2024-10-31")

        assert server.calls["file"] == 0
        assert downloaded.parquet_path.read_bytes() == before

    def test_intermediate_files_are_discarded(self, downloaded: SegmentRunner) -> None:
        downloaded.update(self.next_month(downloaded), "2024-09-01", "2024-10-31")

        assert not downloaded.staging_dir.exists()

    def test_missing_local_file_raises_file_not_found_error(
        self, runner: SegmentRunner
    ) -> None:
        with pytest.raises(FileNotFoundError, match="download it first"):
            runner.update(FakeServer(b""), "2024-09-01", "2024-10-31")

    def test_empty_window_raises_value_error(self, downloaded: SegmentRunner) -> None:
        with pytest.raises(ValueError, match="empty window"):
            downloaded.update(FakeServer(b""), "2024-10-31", "2024-09-01")


class TestLastUpdatedDate:
    def test_date_of_the_timestamp(self) -> None:
        stamp = "2026-10-13T09:29:37.000+00:00"

        assert england_and_wales.last_updated_date(stamp) == dt.date(2026, 10, 13)
