"""
Tests for ``epc_connector.api``.
"""

import datetime as dt
import logging
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from epc_connector import api, sources
from epc_connector.canonical import GB_PARQUET_NAME, gb_origins
from epc_connector.credentials import API_TOKEN_ENV_VAR, ApiTokenError
from epc_connector.datasets import (
    LAST_UPDATED_METADATA_KEY,
    RELEASE_METADATA_KEY,
    Origin,
    Segment,
)
from epc_connector.pipeline import run
from tests.fakes import ENGLAND_AND_WALES_ROW, SCOTLAND_ROW, write_raw_domestic

TODAY = dt.date.today()
FRESH_RELEASE = f"{TODAY.year}q{(TODAY.month - 1) // 3 + 1}"
FRESH_LAST_UPDATED = f"{TODAY.isoformat()}T00:00:00+00:00"


def write_raw(data_dir: Path, origin: Origin, version: str | None = None) -> Path:
    """Write a one-row raw domestic file, as a download would."""
    if origin is Origin.SCOTLAND:
        metadata = {RELEASE_METADATA_KEY: version or FRESH_RELEASE}
        return write_raw_domestic(data_dir, origin, [SCOTLAND_ROW], metadata)
    metadata = {LAST_UPDATED_METADATA_KEY: version or FRESH_LAST_UPDATED}
    return write_raw_domestic(data_dir, origin, [ENGLAND_AND_WALES_ROW], metadata)


class FakeDownloads:
    """Stand-in for ``sources.download`` that records each call."""

    def __init__(self) -> None:
        self.calls: list[tuple[Origin, Segment]] = []
        self.options: list[dict[str, Any]] = []
        self.failing: set[Origin] = set()

    def __call__(
        self, origin: Origin, segment: Segment, *, data_dir: Path, **options: Any
    ) -> Path:
        self.calls.append((Origin(origin), Segment(segment)))
        self.options.append(options)
        if origin in self.failing:
            raise ApiTokenError("no API token")
        return write_raw(data_dir, Origin(origin))


@pytest.fixture
def downloads(monkeypatch: pytest.MonkeyPatch) -> FakeDownloads:
    fake = FakeDownloads()
    monkeypatch.setattr(sources, "download", fake)
    monkeypatch.setenv(API_TOKEN_ENV_VAR, "env-token")
    monkeypatch.setattr(run, "check_staging_space", lambda staging_dir: None)
    return fake


class TestLoadDomesticCertificates:
    def test_first_load_sets_up_only_the_origin_asked_for(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        homes = api.load_domestic_certificates("scotland", data_dir=tmp_path)

        assert downloads.calls == [(Origin.SCOTLAND, Segment.DOMESTIC)]
        assert homes["source_dataset"].to_list() == ["scotland/domestic"]
        assert sorted(p.name for p in tmp_path.glob("*.parquet")) == [
            "domestic_epc_certificate_gb.parquet",
            "domestic_epc_certificate_scotland.parquet",
        ]

    def test_second_load_downloads_nothing(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        api.load_domestic_certificates("scotland", data_dir=tmp_path)
        api.load_domestic_certificates("scotland", data_dir=tmp_path)

        assert len(downloads.calls) == 1

    def test_gb_load_sets_up_both_origins(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        homes = api.load_domestic_certificates(data_dir=tmp_path)

        assert {origin for origin, _ in downloads.calls} == set(Origin)
        assert homes.height == 2

    def test_gb_load_keeps_the_origin_that_could_be_set_up(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        downloads.failing.add(Origin.ENGLAND_AND_WALES)

        with caplog.at_level(logging.WARNING):
            homes = api.load_domestic_certificates("gb", data_dir=tmp_path)

        assert homes["source_dataset"].to_list() == ["scotland/domestic"]
        assert "england-and-wales domestic data could not be set up" in caplog.text
        assert "GB domestic data holds scotland only" in caplog.text

    def test_one_origin_that_cannot_be_set_up_raises(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        downloads.failing.add(Origin.ENGLAND_AND_WALES)

        with pytest.raises(ApiTokenError):
            api.load_domestic_certificates("england-and-wales", data_dir=tmp_path)

    def test_no_origin_set_up_raises(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        downloads.failing.update(Origin)

        with pytest.raises(api.NotSetUpError):
            api.load_domestic_certificates(data_dir=tmp_path)

    def test_api_token_reaches_only_england_and_wales_downloads(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(API_TOKEN_ENV_VAR)

        api.load_domestic_certificates(data_dir=tmp_path, api_token="arg-token")

        tokens = {
            origin: options.get("api_token")
            for (origin, _), options in zip(
                downloads.calls, downloads.options, strict=True
            )
        }
        assert tokens == {Origin.ENGLAND_AND_WALES: "arg-token", Origin.SCOTLAND: None}

    def test_without_any_token_england_and_wales_raises(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(API_TOKEN_ENV_VAR)

        with pytest.raises(ApiTokenError, match="pass api_token"):
            api.load_domestic_certificates("england-and-wales", data_dir=tmp_path)

        assert downloads.calls == []

    def test_raw_file_newer_than_the_gb_rows_is_rebuilt(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        write_raw(tmp_path, Origin.SCOTLAND, "2026q1")
        api.load_domestic_certificates("scotland", data_dir=tmp_path)
        write_raw(tmp_path, Origin.SCOTLAND, FRESH_RELEASE)

        api.load_domestic_certificates("scotland", data_dir=tmp_path)

        assert gb_origins(tmp_path) == {Origin.SCOTLAND: FRESH_RELEASE}
        assert downloads.calls == []

    def test_old_data_warns(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        write_raw(tmp_path, Origin.SCOTLAND, "2020q1")

        with caplog.at_level(logging.WARNING):
            api.load_domestic_certificates("scotland", data_dir=tmp_path)

        assert "scotland domestic data runs to 2020-03-31" in caplog.text

    def test_recent_data_does_not_warn(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        write_raw(tmp_path, Origin.SCOTLAND)
        write_raw(tmp_path, Origin.ENGLAND_AND_WALES)

        with caplog.at_level(logging.WARNING):
            api.load_domestic_certificates(data_dir=tmp_path)

        assert caplog.text == ""

    def test_first_download_is_announced_once(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING):
            api.load_domestic_certificates("scotland", data_dir=tmp_path)
            api.load_domestic_certificates("scotland", data_dir=tmp_path)

        notices = [r for r in caplog.records if "for the first time" in r.message]
        assert len(notices) == 1
        assert "scotland/domestic" in notices[0].message

    def test_without_download_if_missing_a_missing_origin_raises(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        with pytest.raises(api.NotSetUpError, match="download_if_missing"):
            api.load_domestic_certificates(
                "scotland", data_dir=tmp_path, download_if_missing=False
            )

        assert downloads.calls == []

    def test_without_download_if_missing_gb_data_uses_what_is_there(
        self,
        downloads: FakeDownloads,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        write_raw(tmp_path, Origin.SCOTLAND)

        with caplog.at_level(logging.WARNING):
            homes = api.load_domestic_certificates(
                data_dir=tmp_path, download_if_missing=False
            )

        assert downloads.calls == []
        assert homes["source_dataset"].to_list() == ["scotland/domestic"]
        assert "england-and-wales/domestic is not downloaded" in caplog.text

    def test_columns_loads_only_those(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        homes = api.load_domestic_certificates(
            "scotland", columns=["uprn", "country"], data_dir=tmp_path
        )

        assert homes.columns == ["uprn", "country"]

    def test_unknown_column_raises(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        with pytest.raises(api.UnknownColumnError, match="no_such_column"):
            api.load_domestic_certificates(
                "scotland", columns=["uprn", "no_such_column"], data_dir=tmp_path
            )

    def test_lazy_returns_a_lazy_frame(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        homes = api.load_domestic_certificates("scotland", lazy=True, data_dir=tmp_path)

        assert isinstance(homes, pl.LazyFrame)
        assert homes.collect().height == 1

    def test_default_data_directory_is_used(
        self, downloads: FakeDownloads, default_data_dir: Path
    ) -> None:
        api.load_domestic_certificates("scotland")

        assert (default_data_dir / GB_PARQUET_NAME).is_file()


class TestUpdate:
    def test_updates_downloaded_datasets_then_the_gb_file(
        self, downloads: FakeDownloads, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        api.load_domestic_certificates("scotland", data_dir=tmp_path)
        updates: list[tuple[Any, ...]] = []

        def fake_update(origin: Origin, segment: Segment, **options: Any) -> Path:
            updates.append((origin, segment, options["date_from"]))
            return write_raw(options["data_dir"], Origin(origin), FRESH_RELEASE)

        monkeypatch.setattr(sources, "update", fake_update)

        api.update(segments=["domestic"], date_from="2026-01-01", data_dir=tmp_path)

        assert updates == [(Origin.SCOTLAND, Segment.DOMESTIC, "2026-01-01")]
        assert gb_origins(tmp_path) == {Origin.SCOTLAND: FRESH_RELEASE}


class TestStatus:
    def test_reports_what_is_set_up(
        self, downloads: FakeDownloads, tmp_path: Path
    ) -> None:
        api.load_domestic_certificates("scotland", data_dir=tmp_path)

        statuses = {s.name: s for s in api.status(tmp_path)}

        assert statuses["scotland/domestic"].set_up
        assert statuses["scotland/domestic"].version == FRESH_RELEASE
        assert not statuses["england-and-wales/domestic"].set_up
        assert statuses["gb/domestic"].version == f"scotland {FRESH_RELEASE}"
        assert statuses["gb/display"].path is None


class TestMonthsBefore:
    @pytest.mark.parametrize(
        ("day", "expected"),
        [
            (dt.date(2026, 10, 18), dt.date(2026, 7, 18)),
            (dt.date(2026, 2, 15), dt.date(2025, 11, 15)),
            (dt.date(2026, 5, 31), dt.date(2026, 2, 28)),
        ],
    )
    def test_same_day_or_the_month_end(self, day: dt.date, expected: dt.date) -> None:
        assert api._months_before(day, 3) == expected
