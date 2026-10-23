"""
Every environment variable the package reads, in one place.

All share the ``EPC_CONNECTOR_`` prefix (see ``EnvVar``). Values are read
when needed, never at import, so a variable set while a program runs takes
effect. They are read the same way everywhere: surrounding whitespace is
removed, an empty value counts as unset, and paths have ``~`` expanded.
Variables with the prefix that the package does not know (usually typos)
are reported once with a warning.
"""

import functools
import logging
import os
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Final

logger = logging.getLogger(__name__)

PREFIX: Final = "EPC_CONNECTOR_"
"""Prefix of every environment variable the package reads."""


class EnvVar(StrEnum):
    """
    The environment variables the package reads.

    Attributes
    ----------
    DATA_DIR
        Data directory, unless ``data_dir`` is given (see ``workspace``).
    API_TOKEN
        England and Wales API token, unless ``api_token`` is given (see
        ``credentials``); for CI and scripts.
    TOKEN_PATH
        File the command line saves the API token to (see ``credentials``).
    """

    DATA_DIR = "EPC_CONNECTOR_DATA_DIR"
    API_TOKEN = "EPC_CONNECTOR_API_TOKEN"
    TOKEN_PATH = "EPC_CONNECTOR_TOKEN_PATH"


def read_text(variable: EnvVar) -> str | None:
    """Return a variable's value, stripped; ``None`` if unset or empty."""
    return os.environ.get(variable, "").strip() or None


def read_path(variable: EnvVar) -> Path | None:
    """Return a variable's value as a path with ``~`` expanded, if set."""
    value = read_text(variable)
    return None if value is None else Path(value).expanduser()


def unknown_variables(environ: Mapping[str, str] | None = None) -> list[str]:
    """Return variables with the package's prefix that it does not read."""
    names = os.environ if environ is None else environ
    known = set(EnvVar)
    return sorted(n for n in names if n.startswith(PREFIX) and n not in known)


def report_unknown_variables() -> None:
    """Warn, once per process and set of names, about unknown variables."""
    if unknown := unknown_variables():
        _warn_once(tuple(unknown))


@functools.cache
def _warn_once(unknown: tuple[str, ...]) -> None:
    logger.warning(
        "Ignoring unknown environment variable(s) %s; known: %s",
        ", ".join(unknown),
        ", ".join(EnvVar),
    )
