"""
Tests for ``epc_connector._environment``.
"""

import logging
import sys
from pathlib import Path

import pytest

from epc_connector import _environment as environment
from epc_connector._environment import EnvVar


class TestEnvVar:
    def test_every_variable_has_the_package_prefix(self) -> None:
        assert all(variable.startswith(environment.PREFIX) for variable in EnvVar)


class TestReadText:
    def test_value_is_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EnvVar.API_TOKEN, "  abc \n")

        assert environment.read_text(EnvVar.API_TOKEN) == "abc"

    @pytest.mark.parametrize("value", ["", "   "])
    def test_empty_value_counts_as_unset(
        self, value: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EnvVar.API_TOKEN, value)

        assert environment.read_text(EnvVar.API_TOKEN) is None

    def test_unset_variable_is_none(self) -> None:
        assert environment.read_text(EnvVar.API_TOKEN) is None

    def test_value_set_at_runtime_is_seen(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        assert environment.read_text(EnvVar.API_TOKEN) is None

        monkeypatch.setenv(EnvVar.API_TOKEN, "later")

        assert environment.read_text(EnvVar.API_TOKEN) == "later"


class TestReadPath:
    def test_home_is_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOME", "/home/someone")
        monkeypatch.setenv(EnvVar.DATA_DIR, " ~/epc ")

        assert environment.read_path(EnvVar.DATA_DIR) == Path("/home/someone/epc")

    def test_empty_value_counts_as_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EnvVar.DATA_DIR, "")

        assert environment.read_path(EnvVar.DATA_DIR) is None


class TestUnknownVariables:
    def test_typos_with_the_prefix_are_listed(self) -> None:
        environ = {
            "EPC_CONNECTOR_DATADIR": "x",
            "EPC_CONNECTOR_DATA_DIR": "y",
            "EPB_API_TOKEN": "z",
            "HOME": "/home/someone",
        }

        assert environment.unknown_variables(environ) == ["EPC_CONNECTOR_DATADIR"]

    def test_unknown_variables_are_reported_once(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("EPC_CONNECTOR_DATADIR", "x")
        environment._warn_once.cache_clear()

        with caplog.at_level(logging.WARNING):
            environment.report_unknown_variables()
            environment.report_unknown_variables()

        assert caplog.text.count("EPC_CONNECTOR_DATADIR") == 1
        assert "EPC_CONNECTOR_DATA_DIR" in caplog.text  # lists the known ones


@pytest.mark.skipif(sys.platform != "linux", reason="XDG paths are Linux only")
class TestUserDirs:
    def test_data_dir_follows_xdg_data_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))

        assert environment.user_data_dir() == tmp_path / "epc-connector"

    def test_config_dir_follows_xdg_config_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))

        assert environment.user_config_dir() == tmp_path / "epc-connector"

    def test_data_is_not_kept_in_the_cache_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))

        assert environment.user_data_dir().is_relative_to(tmp_path / "data")
