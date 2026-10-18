"""
Tests for ``epc_connector.pipeline.cli`` (the ``epc-connector`` command line).
"""

import logging
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from epc_connector import sources
from epc_connector.canonical import gb
from epc_connector.datasets import Origin, Segment
from epc_connector.pipeline import cli, process
from tests.fakes import write_raw_domestic

type Call = tuple[Callable[..., Path], tuple[Any, ...], dict[str, Any]]


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[Call]:
    """Record the steps the command runs instead of running them."""
    recorded: list[Call] = []

    def record(function: Callable[..., Path], /, *args: Any, **kwargs: Any) -> Path:
        recorded.append((function, args, kwargs))
        return Path("data")

    monkeypatch.setattr(process, "run_step", record)
    monkeypatch.setattr(
        cli, "authorised_client", lambda: SimpleNamespace(close=lambda: None)
    )
    monkeypatch.setattr(
        "shutil.disk_usage", lambda path: SimpleNamespace(free=100 * 1024**3)
    )
    return recorded


def summary(calls: list[Call]) -> list[tuple[Callable[..., Path], tuple[Any, ...]]]:
    return [(function, args) for function, args, _ in calls]


class TestMain:
    def test_setup_downloads_everything_then_builds_gb(self, calls: list) -> None:
        status = cli.main(["setup"])

        assert status == 0
        assert [args for _, args in summary(calls)[:-1]] == [
            (Origin.SCOTLAND, Segment.NON_DOMESTIC),
            (Origin.SCOTLAND, Segment.DOMESTIC),
            (Origin.ENGLAND_AND_WALES, Segment.NON_DOMESTIC),
            (Origin.ENGLAND_AND_WALES, Segment.DISPLAY),
            (Origin.ENGLAND_AND_WALES, Segment.DOMESTIC),
        ]
        assert summary(calls)[-1] == (gb.build_gb, ())
        assert calls[-1][2]["strict"] is True

    def test_setup_one_origin_replaces_it_in_gb(self, calls: list) -> None:
        cli.main(["setup", "--origin", "scotland", "--segment", "domestic"])

        assert summary(calls) == [
            (sources.download, (Origin.SCOTLAND, Segment.DOMESTIC)),
            (gb.replace_origin_in_gb, (Origin.SCOTLAND,)),
        ]

    def test_england_alias_warns_and_uses_england_and_wales(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            cli.main(["setup", "--origin", "wales", "--segment", "domestic"])

        assert calls[0][1] == (Origin.ENGLAND_AND_WALES, Segment.DOMESTIC)
        assert "england-and-wales" in caplog.text

    def test_segment_without_gb_file_warns_not_set_up(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            status = cli.main(["setup", "--segment", "display"])

        assert status == 0
        assert summary(calls) == [
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.DISPLAY)),
        ]
        assert "GB display is not set up yet" in caplog.text

    def test_update_passes_the_window_and_options(self, calls: list) -> None:
        cli.main(
            [
                "update",
                "--origin",
                "scotland",
                "--segment",
                "domestic",
                "--date-from",
                "2025-01-01",
                "--data-dir",
                "elsewhere",
                "--lenient",
            ]
        )

        [(update, args, kwargs), (replace, _, gb_kwargs)] = calls
        assert (update, args) == (
            sources.update,
            (Origin.SCOTLAND, Segment.DOMESTIC),
        )
        assert kwargs["date_from"] == "2025-01-01"
        assert kwargs["date_to"] is None
        assert kwargs["data_dir"] == Path("elsewhere")
        assert replace is gb.replace_origin_in_gb
        assert gb_kwargs["strict"] is False

    def test_scottish_domestic_needs_free_staging_space(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "shutil.disk_usage", lambda path: SimpleNamespace(free=1024**3)
        )

        assert cli.main(["setup", "--origin", "scotland"]) == 1
        assert calls == []

    def test_missing_token_stops_before_any_download(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def no_token() -> None:
            raise ValueError("no API token")

        monkeypatch.setattr(cli, "authorised_client", no_token)

        assert cli.main(["setup"]) == 1
        assert calls == []

    def test_scotland_alone_needs_no_token(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def no_token() -> None:
            raise ValueError("no API token")

        monkeypatch.setattr(cli, "authorised_client", no_token)

        assert cli.main(["setup", "--origin", "scotland"]) == 0

    def test_unpublished_pair_fails_with_one_line(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        status = cli.main(["setup", "--origin", "scotland", "--segment", "display"])

        assert status == 1
        assert calls == []
        assert "no display certificates are published for scotland" in caplog.text

    def test_unknown_segment_is_rejected(self) -> None:
        with pytest.raises(SystemExit):
            cli.main(["setup", "--segment", "commercial"])

    def test_status_lists_datasets(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        status = cli.main(["status", "--data-dir", str(tmp_path)])

        output = capsys.readouterr().out
        assert status == 0
        assert "[ ] scotland/domestic" in output
        assert "gb/display" in output

    def test_coverage_reports_the_chosen_element(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"MAIN_FUEL": "mains gas (not community)"}, {"MAIN_FUEL": ""}]
        write_raw_domestic(tmp_path, Origin.SCOTLAND, rows, {})

        status = cli.main(
            [
                "coverage",
                "--data-dir",
                str(tmp_path),
                "--origin",
                "scotland",
                "--element",
                "main-fuel",
            ]
        )

        output = capsys.readouterr().out
        assert status == 0
        assert "rows 2, distinct 2" in output
