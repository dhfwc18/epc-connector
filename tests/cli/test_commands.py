"""
Tests for ``epc_connector.cli.commands`` (the command line).
"""

import contextlib
import logging
from collections.abc import Callable, Generator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from epc_connector import environment, sources
from epc_connector.canonical import gb
from epc_connector.cli import commands, process
from epc_connector.cli.logs import LogOptions
from epc_connector.credentials import ApiTokenError, save_api_token
from epc_connector.datasets import Origin, Segment, get_dataset
from epc_connector.environment import EnvVar
from tests.fakes import write_raw_domestic

pytestmark = pytest.mark.usefixtures("restore_logging")

type Call = tuple[Callable[..., Path], tuple[Any, ...], dict[str, Any]]


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[Call]:
    """Record the steps the command runs instead of running them."""
    recorded: list[Call] = []

    def record(function: Callable[..., Path], /, *args: Any, **kwargs: Any) -> Path:
        recorded.append((function, args, kwargs))
        return Path("data")

    monkeypatch.setattr(process, "run_step", record)
    monkeypatch.setenv(EnvVar.API_TOKEN, "env-token")
    monkeypatch.setattr(commands, "_can_ask", lambda: False)
    monkeypatch.setattr(
        "shutil.disk_usage", lambda path: SimpleNamespace(free=100 * 1024**3)
    )
    return recorded


def summary(calls: list[Call]) -> list[tuple[Callable[..., Path], tuple[Any, ...]]]:
    return [(function, args) for function, args, _ in calls]


class TestMain:
    def test_setup_downloads_everything_then_builds_gb(self, calls: list) -> None:
        status = commands.main(["setup"])

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
        commands.main(["setup", "--origin", "scotland", "--segment", "domestic"])

        assert summary(calls) == [
            (sources.download, (Origin.SCOTLAND, Segment.DOMESTIC)),
            (gb.replace_origin_in_gb, (Origin.SCOTLAND,)),
        ]

    def test_england_alias_warns_and_uses_england_and_wales(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            commands.main(["setup", "--origin", "wales", "--segment", "domestic"])

        assert calls[0][1] == (Origin.ENGLAND_AND_WALES, Segment.DOMESTIC)
        assert "england-and-wales" in caplog.text

    def test_segment_without_gb_file_warns_not_set_up(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            status = commands.main(["setup", "--segment", "display"])

        assert status == 0
        assert summary(calls) == [
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.DISPLAY)),
        ]
        assert "GB display is not set up yet" in caplog.text

    def test_update_passes_the_window_and_options(
        self, calls: list, tmp_path: Path
    ) -> None:
        get_dataset("scotland", "domestic").path(tmp_path).touch()

        commands.main(
            [
                "update",
                "--origin",
                "scotland",
                "--segment",
                "domestic",
                "--date-from",
                "2025-01-01",
                "--data-dir",
                str(tmp_path),
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
        assert kwargs["data_dir"] == tmp_path
        assert replace is gb.replace_origin_in_gb
        assert gb_kwargs["strict"] is False

    def test_scottish_domestic_needs_free_staging_space(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "shutil.disk_usage", lambda path: SimpleNamespace(free=1024**3)
        )

        assert commands.main(["setup", "--origin", "scotland"]) == 1
        assert calls == []

    def test_missing_token_without_a_terminal_stops_before_any_download(
        self,
        calls: list,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.delenv(EnvVar.API_TOKEN)

        assert commands.main(["setup"]) == 1
        assert calls == []
        assert "epc-connector login" in caplog.text

    def test_scotland_alone_needs_no_token(
        self, calls: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(EnvVar.API_TOKEN)

        assert commands.main(["setup", "--origin", "scotland"]) == 0

    def test_unpublished_pair_fails_with_one_line(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        status = commands.main(
            ["setup", "--origin", "scotland", "--segment", "display"]
        )

        assert status == 1
        assert calls == []
        assert "no display certificates are published for scotland" in caplog.text

    def test_each_step_is_numbered_at_info(
        self, calls: list, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            commands.main(["setup", "--origin", "scotland", "--segment", "domestic"])

        assert "Step 1 of 2: download scotland/domestic" in caplog.text
        assert "Step 2 of 2: replace scotland in GB domestic" in caplog.text
        assert "Finished 2 steps" in caplog.text

    def test_log_options_reach_every_step(self, calls: list, tmp_path: Path) -> None:
        log_file = tmp_path / "run.log"

        commands.main(
            ["setup", "--segment", "domestic", "--debug", "--log-file", str(log_file)]
        )

        expected = LogOptions(debug=True, log_file=log_file)
        assert [kwargs["log_options"] for _, _, kwargs in calls] == [expected] * 3

    def test_error_is_one_line_with_details_in_the_log_file(
        self, calls: list, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        log_file = tmp_path / "run.log"

        status = commands.main(
            [
                "setup",
                "--origin",
                "scotland",
                "--segment",
                "display",
                "--log-file",
                str(log_file),
            ]
        )

        console = capsys.readouterr().err
        assert status == 1
        assert "ERROR   no display certificates are published" in console
        assert "Traceback" not in console
        assert "Traceback" in log_file.read_text()

    def test_second_setup_downloads_nothing(
        self, calls: list, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        get_dataset("scotland", "domestic").path(tmp_path).touch()
        (tmp_path / gb.GB_PARQUET_NAME).touch()
        argv = ["setup", "--origin", "scotland", "--segment", "domestic"]

        with caplog.at_level(logging.INFO):
            status = commands.main([*argv, "--data-dir", str(tmp_path)])

        assert status == 0
        assert calls == []
        assert "scotland/domestic is already downloaded" in caplog.text
        assert "Nothing to do" in caplog.text

    def test_force_downloads_again(self, calls: list, tmp_path: Path) -> None:
        get_dataset("scotland", "domestic").path(tmp_path).touch()
        argv = ["setup", "--origin", "scotland", "--segment", "domestic", "--force"]

        commands.main([*argv, "--data-dir", str(tmp_path)])

        assert summary(calls)[0] == (
            sources.download,
            (Origin.SCOTLAND, Segment.DOMESTIC),
        )

    def test_data_directory_defaults_to_the_environment(
        self, calls: list, default_data_dir: Path
    ) -> None:
        commands.main(["setup", "--origin", "scotland", "--segment", "domestic"])

        kwargs = calls[0][2]
        assert kwargs["data_dir"] == default_data_dir
        assert kwargs["staging_dir"] == default_data_dir / ".staging"

    def test_update_leaves_undownloaded_datasets_alone(
        self, calls: list, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        get_dataset("scotland", "domestic").path(tmp_path).touch()

        with caplog.at_level(logging.WARNING):
            status = commands.main(["update", "--data-dir", str(tmp_path)])

        assert status == 0
        assert summary(calls) == [
            (sources.update, (Origin.SCOTLAND, Segment.DOMESTIC)),
            (gb.replace_origin_in_gb, (Origin.SCOTLAND,)),
        ]
        assert "england-and-wales/domestic is not downloaded" in caplog.text

    def test_setup_plans_and_runs_under_the_data_directory_lock(
        self, calls: list, monkeypatch: pytest.MonkeyPatch, default_data_dir: Path
    ) -> None:
        locked: list[Path] = []

        @contextlib.contextmanager
        def recording_lock(data_dir: Path) -> Generator[Path]:
            locked.append(data_dir)
            yield data_dir

        monkeypatch.setattr(commands, "lock_data_dir", recording_lock)

        commands.main(["setup", "--origin", "scotland", "--segment", "domestic"])

        assert locked == [default_data_dir]

    def test_unknown_environment_variable_is_reported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("EPC_CONNECTOR_DATADIR", str(tmp_path))
        environment._warn_once.cache_clear()

        with caplog.at_level(logging.WARNING):
            commands.main(["status", "--data-dir", str(tmp_path)])

        assert "Ignoring unknown environment variable(s) EPC_CONNECTOR_DATADIR" in (
            caplog.text
        )

    def test_unknown_segment_is_rejected(self) -> None:
        with pytest.raises(SystemExit):
            commands.main(["setup", "--segment", "commercial"])

    def test_status_lists_datasets(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        status = commands.main(["status", "--data-dir", str(tmp_path)])

        output = capsys.readouterr().out
        assert status == 0
        assert "[ ] scotland/domestic" in output
        assert "gb/display" in output

    def test_coverage_reports_the_chosen_element(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"MAIN_FUEL": "mains gas (not community)"}, {"MAIN_FUEL": ""}]
        write_raw_domestic(tmp_path, Origin.SCOTLAND, rows, {})

        status = commands.main(
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


class Prompt:
    """Stand-in for typing a token and for the API checking it."""

    def __init__(self, typed: str = " typed-token ") -> None:
        self.typed = typed
        self.asked = 0
        self.verified: list[str] = []
        self.rejected = False

    def getpass(self, prompt: str = "") -> str:
        self.asked += 1
        return self.typed

    def verify(self, api_token: str) -> None:
        self.verified.append(api_token)
        if self.rejected:
            raise ApiTokenError("the England and Wales API rejected this token")


@pytest.fixture
def prompt(calls: list, monkeypatch: pytest.MonkeyPatch) -> Prompt:
    """No token anywhere, someone at a terminal, and a fake API check."""
    fake = Prompt()
    monkeypatch.delenv(EnvVar.API_TOKEN)
    monkeypatch.setattr(commands, "_can_ask", lambda: True)
    monkeypatch.setattr(commands.getpass, "getpass", fake.getpass)
    monkeypatch.setattr(commands, "verify_api_token", fake.verify)
    return fake


EW_DOMESTIC = ["setup", "--origin", "england-and-wales", "--segment", "domestic"]


class TestApiTokenPrompt:
    def test_missing_token_is_asked_for_checked_and_saved(
        self, prompt: Prompt, calls: list, saved_token_path: Path
    ) -> None:
        assert commands.main(EW_DOMESTIC) == 0

        assert prompt.verified == ["typed-token"]
        assert saved_token_path.read_text() == "typed-token\n"
        assert saved_token_path.stat().st_mode & 0o777 == 0o600
        assert calls

    def test_saved_token_is_not_asked_for_again(
        self, prompt: Prompt, calls: list
    ) -> None:
        save_api_token("saved-token")

        assert commands.main(EW_DOMESTIC) == 0
        assert prompt.asked == 0

    def test_environment_token_is_not_asked_for(
        self, prompt: Prompt, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EnvVar.API_TOKEN, "env-token")

        assert commands.main(EW_DOMESTIC) == 0
        assert prompt.asked == 0

    def test_rejected_token_is_not_saved(
        self, prompt: Prompt, calls: list, saved_token_path: Path
    ) -> None:
        prompt.rejected = True

        assert commands.main(EW_DOMESTIC) == 1
        assert not saved_token_path.exists()
        assert calls == []

    def test_scotland_alone_is_not_asked(self, prompt: Prompt) -> None:
        assert commands.main(["setup", "--origin", "scotland"]) == 0
        assert prompt.asked == 0

    def test_login_replaces_the_saved_token(
        self, prompt: Prompt, saved_token_path: Path
    ) -> None:
        save_api_token("old-token")

        assert commands.main(["login"]) == 0
        assert saved_token_path.read_text() == "typed-token\n"

    def test_logout_deletes_the_saved_token(
        self, prompt: Prompt, saved_token_path: Path
    ) -> None:
        save_api_token("old-token")

        assert commands.main(["logout"]) == 0
        assert not saved_token_path.exists()

    def test_logout_without_a_saved_token_says_so(
        self, prompt: Prompt, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            assert commands.main(["logout"]) == 0

        assert "No saved API token" in caplog.text

    def test_login_without_a_terminal_fails_with_one_line(
        self,
        prompt: Prompt,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        saved_token_path: Path,
    ) -> None:
        monkeypatch.setattr(commands, "_can_ask", lambda: False)

        assert commands.main(["login"]) == 1

        console = capsys.readouterr().err
        assert "login needs a terminal" in console
        assert "Traceback" not in console
        assert prompt.asked == 0
        assert not saved_token_path.exists()

    def test_end_of_input_at_the_prompt_is_no_token(
        self,
        prompt: Prompt,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        saved_token_path: Path,
    ) -> None:
        def end_of_input(prompt_text: str = "") -> str:
            raise EOFError

        monkeypatch.setattr(commands.getpass, "getpass", end_of_input)

        assert commands.main(["login"]) == 1

        console = capsys.readouterr().err
        assert "no token entered" in console
        assert "Traceback" not in console
        assert not saved_token_path.exists()
