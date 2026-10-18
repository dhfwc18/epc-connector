"""
Tests for ``epc_connector.pipeline.logs``.
"""

import logging
from pathlib import Path

import httpx
import pytest

from epc_connector.pipeline import logs

pytestmark = pytest.mark.usefixtures("restore_logging")

package = logging.getLogger("epc_connector.storage.files")
library = logging.getLogger("httpx")


class TestConfigureLogging:
    def test_default_console_shows_package_info_but_not_debug(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        logs.configure_logging()

        package.info("milestone")
        package.debug("detail")

        console = capsys.readouterr().err
        assert "INFO    milestone" in console
        assert "detail" not in console

    def test_default_console_shows_only_warnings_from_libraries(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        logs.configure_logging()

        library.info("HTTP Request: GET ...")
        library.warning("connection reset")

        console = capsys.readouterr().err
        assert "HTTP Request" not in console
        assert "WARNING connection reset" in console

    def test_debug_console_shows_everything_with_its_module(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        logs.configure_logging(logs.LogOptions(debug=True))

        package.debug("detail")
        library.info("HTTP Request: GET ...")

        console = capsys.readouterr().err
        assert "epc_connector.storage.files: detail" in console
        assert "httpx: HTTP Request: GET ..." in console

    def test_log_file_records_debug_whatever_the_console_shows(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        log_file = tmp_path / "logs" / "run.log"
        logs.configure_logging(logs.LogOptions(log_file=log_file))

        package.debug("detail")
        library.info("HTTP Request: GET ...")

        recorded = log_file.read_text()
        assert "detail" in recorded
        assert "HTTP Request" in recorded
        assert "detail" not in capsys.readouterr().err

    def test_calling_again_replaces_the_handlers(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        logs.configure_logging()
        logs.configure_logging()

        package.info("once")

        assert capsys.readouterr().err.count("once") == 1

    def test_calling_again_closes_the_previous_log_file(self, tmp_path: Path) -> None:
        logs.configure_logging(logs.LogOptions(log_file=tmp_path / "run.log"))
        [log_file] = [
            h for h in logging.getLogger().handlers if h.get_name() == logs.FILE_HANDLER
        ]

        logs.configure_logging()

        assert log_file not in logging.getLogger().handlers
        assert isinstance(log_file, logging.FileHandler)
        assert log_file.stream is None

    def test_handlers_added_by_others_are_kept(self) -> None:
        other = logging.NullHandler()
        logging.getLogger().addHandler(other)

        logs.configure_logging()
        logs.configure_logging()

        assert other in logging.getLogger().handlers

    def test_query_of_signed_urls_is_hidden(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        logs.configure_logging(logs.LogOptions(debug=True))
        signed = httpx.URL("https://bucket.s3.amazonaws.com/f.zip?X-Amz-Signature=s")
        search = httpx.URL("https://api.example/search?date_start=2026-01-01")

        library.info("HTTP Request: GET %s", signed)
        library.info("HTTP Request: GET %s", search)

        console = capsys.readouterr().err
        assert "X-Amz-Signature" not in console
        assert "https://bucket.s3.amazonaws.com/f.zip" in console
        assert "date_start=2026-01-01" in console
