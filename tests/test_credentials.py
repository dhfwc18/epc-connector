"""
Tests for ``epc_connector.credentials``.
"""

from pathlib import Path

import pytest

from epc_connector import credentials


class TestResolveApiToken:
    def test_argument_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(credentials.API_TOKEN_ENV_VAR, "env")
        credentials.save_api_token("saved")

        assert credentials.resolve_api_token("argument") == "argument"

    def test_environment_wins_over_the_saved_token(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(credentials.API_TOKEN_ENV_VAR, "env")
        credentials.save_api_token("saved")

        assert credentials.resolve_api_token() == "env"

    def test_saved_token_is_used_last(self) -> None:
        credentials.save_api_token("saved")

        assert credentials.resolve_api_token() == "saved"

    def test_nothing_found_returns_none(self) -> None:
        assert credentials.resolve_api_token() is None

    def test_dotenv_file_is_not_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / ".env").write_text("EPB_API_TOKEN=from-dotenv\n")
        monkeypatch.chdir(tmp_path)

        assert credentials.resolve_api_token() is None


class TestRequireApiToken:
    def test_missing_token_says_how_to_give_one(self) -> None:
        with pytest.raises(credentials.ApiTokenError) as error:
            credentials.require_api_token()

        for hint in ["api_token", "EPB_API_TOKEN", "epc-connector login"]:
            assert hint in str(error.value)

    def test_error_is_still_a_value_error(self) -> None:
        assert issubclass(credentials.ApiTokenError, ValueError)


class TestCheckedApiToken:
    def test_surrounding_spaces_are_removed(self) -> None:
        assert credentials.checked_api_token("  abc-123\n") == "abc-123"

    @pytest.mark.parametrize(
        "value", ["", "   ", "two words", "caf\N{LATIN SMALL LETTER E WITH ACUTE}"]
    )
    def test_values_that_cannot_be_tokens_raise(self, value: str) -> None:
        with pytest.raises(credentials.ApiTokenError):
            credentials.checked_api_token(value)


class TestSaveApiToken:
    def test_token_is_readable_by_the_user_only(self, saved_token_path: Path) -> None:
        path = credentials.save_api_token(" abc ")

        assert path == saved_token_path
        assert path.read_text() == "abc\n"
        assert path.stat().st_mode & 0o777 == 0o600

    def test_existing_file_is_replaced_and_locked_down(
        self, saved_token_path: Path
    ) -> None:
        saved_token_path.parent.mkdir(parents=True)
        saved_token_path.write_text("old\n")
        saved_token_path.chmod(0o644)

        credentials.save_api_token("new")

        assert saved_token_path.read_text() == "new\n"
        assert saved_token_path.stat().st_mode & 0o777 == 0o600


class TestDeleteApiToken:
    def test_saved_token_is_deleted(self, saved_token_path: Path) -> None:
        credentials.save_api_token("abc")

        assert credentials.delete_api_token() is True
        assert not saved_token_path.exists()

    def test_nothing_saved_returns_false(self) -> None:
        assert credentials.delete_api_token() is False


class TestTokenPath:
    def test_environment_variable_moves_the_file(self, saved_token_path: Path) -> None:
        assert credentials.token_path() == saved_token_path

    def test_default_is_in_the_user_configuration_directory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(credentials.TOKEN_PATH_ENV_VAR)

        path = credentials.token_path()

        assert path.name == "token"
        assert path.parent.name == "epc-connector"
