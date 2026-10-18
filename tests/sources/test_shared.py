"""
Tests for ``epc_connector.sources._shared``.
"""

import datetime as dt

import httpx
import pytest

from epc_connector.sources._shared import as_date, client_or_new


class TestAsDate:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("2026-09-01", dt.date(2026, 9, 1)),
            (dt.date(2026, 9, 1), dt.date(2026, 9, 1)),
            (None, None),
        ],
    )
    def test_value_becomes_a_date(
        self, value: dt.date | str | None, expected: dt.date | None
    ) -> None:
        assert as_date(value) == expected


class TestClientOrNew:
    def test_given_client_is_used_and_left_open(self) -> None:
        given = httpx.Client()

        with client_or_new(given, httpx.Client) as client:
            assert client is given

        assert not given.is_closed
        given.close()

    def test_created_client_is_closed_afterwards(self) -> None:
        with client_or_new(None, httpx.Client) as client:
            assert not client.is_closed

        assert client.is_closed
