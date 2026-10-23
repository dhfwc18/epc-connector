"""
Tests for ``epc_connector.sources.scotland``.
"""

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from epc_connector import validation
from epc_connector.datasets import DatasetError, Origin, Segment, select_datasets
from epc_connector.io import StorageError
from epc_connector.io.archives import extract_files
from epc_connector.sources import scotland
from tests.fakes import (
    SOURCE_URL,
    FakeServer,
    abc_schema,
    created_csv,
    created_schema,
    scottish_csv,
    write_7z,
)

URL_CONSTANTS = {
    "domestic": scotland.DOMESTIC_URL,
    "non-domestic": scotland.NON_DOMESTIC_URL,
}


class SegmentRunner:
    """Run the domestic segment against a fake server inside ``tmp_path``."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.data_dir = tmp_path / "data"
        self.staging_dir = tmp_path / ".staging"
        self.schemas_dir = tmp_path / "schemas"
        self.use_schema(abc_schema())

    @property
    def parquet_path(self) -> Path:
        return self.data_dir / "domestic_epc_certificate_scotland.parquet"

    def use_schema(self, schema: dict[str, Any]) -> None:
        path = self.schemas_dir / "source" / "scotland" / "domestic.schema.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(schema))

    def server(self, members: dict[str, bytes], **options: Any) -> FakeServer:
        archive = write_7z(self.tmp_path / "source.7z", members)
        return FakeServer(archive.read_bytes(), **options)

    def download(self, server: FakeServer) -> Path:
        with server.client() as client:
            return scotland.download(
                "domestic",
                data_dir=self.data_dir,
                staging_dir=self.staging_dir,
                schemas_dir=self.schemas_dir,
                client=client,
            )

    def update(
        self,
        server: FakeServer,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> Path:
        with server.client() as client:
            return scotland.update(
                "domestic",
                date_from=date_from,
                date_to=date_to,
                data_dir=self.data_dir,
                staging_dir=self.staging_dir,
                schemas_dir=self.schemas_dir,
                client=client,
            )


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> SegmentRunner:
    monkeypatch.setitem(scotland.SOURCE_URLS, Segment.DOMESTIC, SOURCE_URL)
    return SegmentRunner(tmp_path)


def title(release: str) -> str:
    """Return a dataset title for a release such as ``"Q1 2026"``."""
    return f"Domestic Energy Performance Certificates - Dataset to {release}"


def values_by_report(parquet_path: Path) -> dict[str, str]:
    frame = pl.read_parquet(parquet_path)
    return dict(zip(frame["REPORT_REFERENCE_NUMBER"], frame["VALUE"], strict=True))


class TestSourceConstants:
    @pytest.mark.parametrize("url", URL_CONSTANTS.values(), ids=URL_CONSTANTS.keys())
    def test_url_points_at_a_ckan_resource_download(self, url: str) -> None:
        assert re.search(r"/resource/[0-9a-f-]{36}/download$", url)

    def test_every_published_segment_has_a_url(self) -> None:
        published = {d.segment for d in select_datasets([Origin.SCOTLAND])}

        assert set(scotland.SOURCE_URLS) == published


class TestReleaseFromTitle:
    def test_quarter_and_year_become_release(self) -> None:
        assert scotland.release_from_title(title("Q2 2026")) == "2026q2"

    def test_title_without_quarter_raises_storage_error(self) -> None:
        with pytest.raises(StorageError, match="no release quarter"):
            scotland.release_from_title("Domestic Energy Performance Certificates")


class TestReleaseEndDate:
    @pytest.mark.parametrize(
        ("release", "expected"),
        [
            ("2026q1", dt.date(2026, 3, 31)),
            ("2026q2", dt.date(2026, 6, 30)),
            ("2026q4", dt.date(2026, 12, 31)),
        ],
    )
    def test_last_day_of_the_quarter(self, release: str, expected: dt.date) -> None:
        assert scotland.release_end_date(release) == expected


class TestLatestCreatedDate:
    def test_future_dates_are_ignored_as_entry_errors(self, tmp_path: Path) -> None:
        path = tmp_path / "segment.parquet"
        created = ["01/02/2025 10:00:00", "02/01/2100 09:00:00", "15/06/2025 08:30:00"]
        pl.DataFrame({"CREATED_AT": created}).write_parquet(path)

        latest = scotland.latest_created_date(path, today=dt.date(2026, 10, 16))

        assert latest == dt.date(2025, 6, 15)

    def test_unparseable_dates_are_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "segment.parquet"
        pl.DataFrame({"CREATED_AT": ["", "15/06/2025 08:30:00"]}).write_parquet(path)

        assert scotland.latest_created_date(path) == dt.date(2025, 6, 15)


class TestPeriodSortKey:
    def test_years_sort_oldest_first_after_undated_files(self) -> None:
        names = ["2017.csv", "2008.csv", "AllYears.csv", "2026.csv"]

        ordered = sorted((Path(n) for n in names), key=scotland.period_sort_key)

        assert [p.name for p in ordered] == [
            "AllYears.csv",
            "2008.csv",
            "2017.csv",
            "2026.csv",
        ]


class TestDownload:
    def test_segment_is_merged_into_named_parquet_file(
        self, runner: SegmentRunner, yearly_archive: Path
    ) -> None:
        output = runner.download(FakeServer(yearly_archive.read_bytes()))

        assert output == runner.parquet_path
        assert pl.read_parquet(output)["A"].to_list() == ["1", "2", "3", "4"]

    def test_release_is_stored_in_parquet_metadata(
        self, runner: SegmentRunner, yearly_archive: Path
    ) -> None:
        output = runner.download(FakeServer(yearly_archive.read_bytes()))

        assert scotland.stored_release(output) == "2026q2"

    def test_staging_directory_is_removed_after_success(
        self, runner: SegmentRunner, yearly_archive: Path
    ) -> None:
        runner.download(FakeServer(yearly_archive.read_bytes()))

        assert not runner.staging_dir.exists()

    def test_new_data_directory_is_marked_ignored(
        self, runner: SegmentRunner, yearly_archive: Path
    ) -> None:
        runner.download(FakeServer(yearly_archive.read_bytes()))

        assert (runner.data_dir / ".gitignore").read_text().splitlines()[-1] == "*"
        assert (runner.data_dir / "CACHEDIR.TAG").is_file()

    def test_default_directories_are_used(
        self, runner: SegmentRunner, yearly_archive: Path, default_data_dir: Path
    ) -> None:
        with FakeServer(yearly_archive.read_bytes()).client() as client:
            output = scotland.download(
                "domestic", schemas_dir=runner.schemas_dir, client=client
            )

        assert output.parent == default_data_dir
        assert not (default_data_dir / ".staging").exists()

    def test_existing_parquet_file_is_replaced(
        self, runner: SegmentRunner, yearly_archive: Path
    ) -> None:
        server = FakeServer(yearly_archive.read_bytes())
        output = runner.download(server)
        output.write_bytes(b"stale")

        runner.download(server)

        assert pl.read_parquet(output).height == 4

    def test_staging_directory_is_removed_after_failure(
        self, runner: SegmentRunner
    ) -> None:
        with pytest.raises(StorageError):
            runner.download(runner.server({"note.pdf": b"%PDF"}))

        assert not runner.staging_dir.exists()
        assert not runner.parquet_path.exists()

    def test_data_breaking_the_schema_is_not_published(
        self, runner: SegmentRunner
    ) -> None:
        server = runner.server({"2024.csv": scottish_csv(["one,b,x"])})

        with pytest.raises(StorageError, match="A: 1 invalid"):
            runner.download(server)

        assert not runner.parquet_path.exists()
        assert not runner.staging_dir.exists()

    def test_unknown_segment_raises_dataset_error(self) -> None:
        with pytest.raises(DatasetError, match="not-a-segment"):
            scotland.download("not-a-segment")

    def test_unpublished_segment_raises_dataset_error(self) -> None:
        with pytest.raises(DatasetError, match="does not publish display"):
            scotland.download("display")


class TestUpdate:
    @pytest.fixture
    def downloaded(self, runner: SegmentRunner) -> SegmentRunner:
        """A local segment at release Q4 2025 holding 2024 and 2025 rows."""
        runner.use_schema(created_schema())
        members = {
            "2024.csv": created_csv([("r1", "10/03/2024 09:00:00", "old")]),
            "2025.csv": created_csv(
                [
                    ("r2", "10/03/2025 09:00:00", "old"),
                    ("r3", "20/12/2025 09:00:00", "old"),
                ]
            ),
        }
        runner.download(runner.server(members, title=title("Q4 2025")))
        return runner

    @staticmethod
    def new_release(runner: SegmentRunner, **options: Any) -> FakeServer:
        """Q1 2026 release: every row re-published as "new", plus r4 and r5."""
        members = {
            "2024.csv": created_csv([("r1", "10/03/2024 09:00:00", "new")]),
            "2025.csv": created_csv(
                [
                    ("r2", "10/03/2025 09:00:00", "new"),
                    ("r3", "20/12/2025 09:00:00", "new"),
                    ("r4", "20/12/2025 17:00:00", "new"),
                ]
            ),
            "2026.csv": created_csv([("r5", "05/02/2026 11:00:00", "new")]),
        }
        return runner.server(members, title=title("Q1 2026"), **options)

    def test_new_release_refreshes_rows_from_latest_local_date(
        self, downloaded: SegmentRunner
    ) -> None:
        downloaded.update(self.new_release(downloaded))

        assert values_by_report(downloaded.parquet_path) == {
            "r1": "old",
            "r2": "old",
            "r3": "new",
            "r4": "new",
            "r5": "new",
        }
        assert scotland.stored_release(downloaded.parquet_path) == "2026q1"

    def test_explicit_window_refreshes_only_those_dates(
        self, downloaded: SegmentRunner
    ) -> None:
        downloaded.update(
            self.new_release(downloaded), date_from="2025-01-01", date_to="2025-06-30"
        )

        assert values_by_report(downloaded.parquet_path) == {
            "r1": "old",
            "r2": "new",
            "r3": "old",
        }

    def test_only_files_overlapping_the_window_are_extracted(
        self, downloaded: SegmentRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        extracted: list[str] = []

        def recording_extract(*args: Any, **kwargs: Any) -> list[Path]:
            paths = extract_files(*args, **kwargs)
            extracted.extend(p.name for p in paths)
            return paths

        monkeypatch.setattr(
            "epc_connector.io.archives.extract_files", recording_extract
        )

        downloaded.update(self.new_release(downloaded), date_from="2026-01-01")

        assert extracted == ["2026.csv"]

    def test_same_release_without_dates_skips_download(
        self, downloaded: SegmentRunner
    ) -> None:
        server = downloaded.server({}, title=title("Q4 2025"))

        downloaded.update(server)

        assert server.calls["file"] == 0

    def test_window_without_matching_years_raises_and_keeps_file(
        self, downloaded: SegmentRunner
    ) -> None:
        before = downloaded.parquet_path.read_bytes()

        with pytest.raises(StorageError, match=r"no \.csv files"):
            downloaded.update(self.new_release(downloaded), date_from="2030-01-01")

        assert downloaded.parquet_path.read_bytes() == before
        assert not downloaded.staging_dir.exists()

    def test_missing_local_file_raises_file_not_found_error(
        self, runner: SegmentRunner
    ) -> None:
        with pytest.raises(FileNotFoundError, match="download it first"):
            runner.update(FakeServer(b""))

    def test_date_to_before_date_from_raises_value_error(
        self, downloaded: SegmentRunner
    ) -> None:
        with pytest.raises(ValueError, match="before"):
            downloaded.update(
                FakeServer(b""), date_from="2025-06-01", date_to="2025-01-01"
            )

    def test_staging_directory_is_removed_after_update(
        self, downloaded: SegmentRunner
    ) -> None:
        downloaded.update(self.new_release(downloaded))

        assert not downloaded.staging_dir.exists()


@pytest.fixture(scope="module")
def domestic_rules() -> dict[str, pl.Expr]:
    """Polars rules for the repository's domestic schema."""
    path = validation.DEFAULT_SCHEMAS_DIR / "source/scotland/domestic.schema.json"
    return validation.column_rules(validation.load_schema(path))


class TestDomesticSchema:
    """Value rules of the repository schema for the domestic segment."""

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
