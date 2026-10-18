"""
Helpers shared by the source modules.
"""

import contextlib
import datetime as dt
from collections.abc import Callable

import httpx


def as_date(value: dt.date | str | None) -> dt.date | None:
    """Return ``value`` as a date; strings must be ``"YYYY-MM-DD"``."""
    if isinstance(value, str):
        return dt.date.fromisoformat(value)
    return value


def client_or_new(
    client: httpx.Client | None, create: Callable[[], httpx.Client]
) -> contextlib.AbstractContextManager[httpx.Client]:
    """
    Return a context manager giving ``client``, or a new one from ``create``.

    A given client is left open for its owner; a created one is closed when
    the ``with`` block ends.
    """
    return contextlib.nullcontext(client) if client is not None else create()
