"""
Tests for ``epc_connector.workspace``.
"""

import logging
import threading
from pathlib import Path

import pytest

from epc_connector import workspace


class TestResolveDataDir:
    def test_given_directory_wins_over_the_environment(self) -> None:
        assert workspace.resolve_data_dir("mine") == Path("mine")

    def test_environment_is_used_when_none_is_given(
        self, default_data_dir: Path
    ) -> None:
        assert workspace.resolve_data_dir() == default_data_dir

    def test_without_either_it_is_epc_data_in_the_working_directory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(workspace.DATA_DIR_ENV_VAR)

        assert workspace.resolve_data_dir() == Path(".epc_data")

    def test_home_is_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOME", "/home/someone")

        assert workspace.resolve_data_dir("~/epc") == Path("/home/someone/epc")


class TestResolveStagingDir:
    def test_default_is_inside_the_data_directory(self, tmp_path: Path) -> None:
        staging = workspace.resolve_staging_dir(None, tmp_path)

        assert staging == tmp_path / ".staging"

    def test_given_directory_is_kept(self, tmp_path: Path) -> None:
        staging = workspace.resolve_staging_dir("/scratch", tmp_path)

        assert staging == Path("/scratch")


class TestPrepareDataDir:
    def test_new_directory_gets_ignore_markers(self, tmp_path: Path) -> None:
        data_dir = workspace.prepare_data_dir(tmp_path / "a" / ".epc_data")

        assert (data_dir / ".gitignore").read_text() == workspace.GITIGNORE_TEXT
        tag = (data_dir / "CACHEDIR.TAG").read_text()
        assert tag.startswith("Signature: 8a477f597d28d172789f06886806bc55\n")

    def test_existing_directory_is_left_unchanged(self, tmp_path: Path) -> None:
        (tmp_path / "own.txt").write_text("mine")

        workspace.prepare_data_dir(tmp_path)

        assert [p.name for p in tmp_path.iterdir()] == ["own.txt"]

    def test_markers_are_ascii(self) -> None:
        assert workspace.GITIGNORE_TEXT.isascii()
        assert workspace.CACHEDIR_TAG_TEXT.isascii()


class TestLockDataDir:
    def test_directory_is_created_and_marked(self, tmp_path: Path) -> None:
        data_dir = tmp_path / ".epc_data"

        with workspace.lock_data_dir(data_dir):
            assert (data_dir / ".gitignore").is_file()

    def test_second_run_waits_for_the_first(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        holding, release = threading.Event(), threading.Event()
        order: list[str] = []

        def first_run() -> None:
            with workspace.lock_data_dir(tmp_path):
                holding.set()
                release.wait(timeout=5)
                order.append("first done")

        thread = threading.Thread(target=first_run)
        thread.start()
        holding.wait(timeout=5)
        threading.Timer(0.2, release.set).start()

        with caplog.at_level(logging.WARNING), workspace.lock_data_dir(tmp_path):
            order.append("second runs")
        thread.join(timeout=5)

        assert order == ["first done", "second runs"]
        assert "waiting for it to finish" in caplog.text

    def test_lock_is_released_after_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(RuntimeError), workspace.lock_data_dir(tmp_path):
            raise RuntimeError("step failed")

        with workspace.lock_data_dir(tmp_path):
            pass  # would wait forever if the lock were still held
