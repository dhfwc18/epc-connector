"""
Tests for ``epc_connector.__main__`` (the ``epc-connector`` command line).
"""

import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from epc_connector import __main__ as cli
from epc_connector.datasets import Origin
from epc_connector.sources import england_and_wales, scotland
from tests.fakes import write_raw_domestic


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, tuple, dict]]:
    """Replace the download, update and merge functions with recorders."""
    recorded: list[tuple[str, tuple, dict]] = []

    def recorder(name: str):
        def record(*args: Any, **kwargs: Any) -> Path:
            recorded.append((name, args, kwargs))
            return Path("data") / name

        return record

    for module, function in [
        (scotland, "download"),
        (scotland, "update"),
        (england_and_wales, "download"),
        (england_and_wales, "update"),
    ]:
        name = f"{module.__name__.rsplit('.', 1)[1]}.{function}"
        monkeypatch.setattr(module, function, recorder(name))
    monkeypatch.setattr(cli, "build_gb", recorder("build_gb"))
    # Run steps inline: a spawned process would not see these replacements.
    monkeypatch.setattr(cli, "_run_step", lambda function, *a, **k: function(*a, **k))
    monkeypatch.setattr(
        cli, "authorised_client", lambda: SimpleNamespace(close=lambda: None)
    )
    monkeypatch.setattr(
        "shutil.disk_usage", lambda path: SimpleNamespace(free=100 * 1024**3)
    )
    return recorded


class TestMain:
    def test_download_runs_each_series_in_order(self, calls: list) -> None:
        status = cli.main(["download", "england-and-wales"])

        assert status == 0
        assert [args[0] for _, args, _ in calls] == [
            "non-domestic",
            "display",
            "domestic",
        ]
        assert {name for name, _, _ in calls} == {"england_and_wales.download"}

    def test_update_passes_the_date_window_and_options(self, calls: list) -> None:
        cli.main(
            [
                "update",
                "scotland",
                "--series",
                "domestic",
                "--date-from",
                "2025-01-01",
                "--data-dir",
                "elsewhere",
            ]
        )

        [(name, args, kwargs)] = calls
        assert (name, args) == ("scotland.update", ("domestic",))
        assert kwargs["date_from"] == "2025-01-01"
        assert kwargs["date_to"] is None
        assert kwargs["data_dir"] == Path("elsewhere")

    def test_merge_is_strict_unless_lenient(self, calls: list) -> None:
        cli.main(["merge"])
        cli.main(["merge", "--lenient"])

        assert [kwargs["strict"] for _, _, kwargs in calls] == [True, False]

    def test_scottish_domestic_needs_free_staging_space(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "shutil.disk_usage", lambda path: SimpleNamespace(free=1024**3)
        )

        assert cli.main(["download", "scotland"]) == 1
        assert calls == []

    def test_setup_downloads_both_nations_then_merges(self, calls: list) -> None:
        status = cli.main(["setup", "--domestic-only", "--lenient"])

        assert status == 0
        assert [(name, args) for name, args, _ in calls] == [
            ("scotland.download", ("domestic",)),
            ("england_and_wales.download", ("domestic",)),
            ("build_gb", ()),
        ]
        assert calls[-1][2]["strict"] is False

    def test_setup_downloads_every_series_by_default(self, calls: list) -> None:
        cli.main(["setup"])

        assert [args for _, args, _ in calls[:-1]] == [
            ("non-domestic",),
            ("domestic",),
            ("non-domestic",),
            ("display",),
            ("domestic",),
        ]

    def test_setup_stops_before_downloading_without_a_token(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def no_token() -> None:
            raise ValueError("no API token")

        monkeypatch.setattr(cli, "authorised_client", no_token)

        assert cli.main(["setup"]) == 1
        assert calls == []

    def test_series_must_belong_to_the_nation(self) -> None:
        with pytest.raises(SystemExit):
            cli.main(["download", "scotland", "--series", "display"])

    def test_coverage_reports_every_column(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"MAIN_FUEL": "mains gas (not community)"}, {"MAIN_FUEL": ""}]
        write_raw_domestic(tmp_path, Origin.SCOTLAND, rows, {})

        status = cli.main(
            [
                "coverage",
                "--data-dir",
                str(tmp_path),
                "--nation",
                "scotland",
                "--element",
                "main-fuel",
            ]
        )

        output = capsys.readouterr().out
        assert status == 0
        assert "rows 2, distinct 2" in output
        assert "main_fuel_type" in output


def path_of_process(name: str) -> Path:
    """Step stand-in: return a path naming the process that ran it."""
    return Path(name) / str(os.getpid())


def failing_step() -> Path:
    raise ValueError("step failed")


class TestRunStep:
    def test_step_runs_in_another_process_and_returns_its_result(self) -> None:
        result = cli._run_step(path_of_process, "out")

        assert result.parent == Path("out")
        assert result.name != str(os.getpid())

    def test_error_in_the_step_is_raised(self) -> None:
        with pytest.raises(ValueError, match="step failed"):
            cli._run_step(failing_step)
