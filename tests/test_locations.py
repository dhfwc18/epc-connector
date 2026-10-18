"""
Tests for ``epc_connector.locations``.
"""

from pathlib import Path

import pytest

from epc_connector import locations


class TestResolveDataDir:
    def test_given_directory_wins_over_the_environment(self) -> None:
        assert locations.resolve_data_dir("mine") == Path("mine")

    def test_environment_is_used_when_none_is_given(
        self, default_data_dir: Path
    ) -> None:
        assert locations.resolve_data_dir() == default_data_dir

    def test_without_either_it_is_epc_data_in_the_working_directory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(locations.DATA_DIR_ENV_VAR)

        assert locations.resolve_data_dir() == Path(".epc_data")

    def test_home_is_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOME", "/home/someone")

        assert locations.resolve_data_dir("~/epc") == Path("/home/someone/epc")


class TestResolveStagingDir:
    def test_default_is_inside_the_data_directory(self, tmp_path: Path) -> None:
        staging = locations.resolve_staging_dir(None, tmp_path)

        assert staging == tmp_path / ".staging"

    def test_given_directory_is_kept(self, tmp_path: Path) -> None:
        staging = locations.resolve_staging_dir("/scratch", tmp_path)

        assert staging == Path("/scratch")


class TestPrepareDataDir:
    def test_new_directory_gets_ignore_markers(self, tmp_path: Path) -> None:
        data_dir = locations.prepare_data_dir(tmp_path / "a" / ".epc_data")

        assert (data_dir / ".gitignore").read_text() == locations.GITIGNORE_TEXT
        tag = (data_dir / "CACHEDIR.TAG").read_text()
        assert tag.startswith("Signature: 8a477f597d28d172789f06886806bc55\n")

    def test_existing_directory_is_left_unchanged(self, tmp_path: Path) -> None:
        (tmp_path / "own.txt").write_text("mine")

        locations.prepare_data_dir(tmp_path)

        assert [p.name for p in tmp_path.iterdir()] == ["own.txt"]

    def test_markers_are_ascii(self) -> None:
        assert locations.GITIGNORE_TEXT.isascii()
        assert locations.CACHEDIR_TAG_TEXT.isascii()
