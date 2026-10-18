"""
Validate tabular data against the JSON Schemas in ``schemas/``.

Each source schema describes one CSV row as an object of strings. Instead of
validating rows one by one, every property is translated into a Polars
expression, so a whole file is checked in one streaming pass. Only the
keywords the source schemas use are supported; any other keyword raises
``UnsupportedSchemaError`` rather than being silently ignored.
"""

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import polars as pl

from epc_connector.errors import EpcConnectorError

DEFAULT_SCHEMAS_DIR: Final = Path(__file__).resolve().parents[2] / "schemas"
"""Repository ``schemas/`` directory."""

# Keywords that describe a property without constraining its value.
_ANNOTATIONS = frozenset({"title", "description", "format"})


class UnsupportedSchemaError(EpcConnectorError, ValueError):
    """A schema uses a keyword this module cannot translate to Polars."""


def load_schema(path: Path) -> dict[str, Any]:
    """Return a JSON Schema loaded from ``path``."""
    return json.loads(path.read_text(encoding="utf-8"))


def schema_columns(schema: Mapping[str, Any]) -> list[str]:
    """Return the schema's column names in order."""
    return list(schema["properties"])


def check_header(header: list[str], schema: Mapping[str, Any]) -> None:
    """
    Raise ``ValueError`` unless ``header`` is exactly the schema's columns.

    Parameters
    ----------
    header
        Column names as read from the file.
    schema
        Source JSON Schema with one property per column.
    """
    expected = schema_columns(schema)
    if header == expected:
        return
    missing = [c for c in expected if c not in header]
    unexpected = [c for c in header if c not in expected]
    if missing or unexpected:
        raise ValueError(f"header mismatch: missing {missing}, unexpected {unexpected}")
    raise ValueError("header has the expected columns in a different order")


def column_rules(schema: Mapping[str, Any]) -> dict[str, pl.Expr]:
    """
    Translate each property of a source schema into an "is valid" expression.

    Supports ``type: string``, ``pattern``, ``enum`` and ``anyOf`` over those
    and over ``$ref`` to ``#/$defs/<name>``.

    Raises
    ------
    UnsupportedSchemaError
        If a property uses any other keyword.
    """
    defs = schema.get("$defs", {})
    return {
        name: _rule(pl.col(name), spec, defs)
        for name, spec in schema["properties"].items()
    }


def _rule(column: pl.Expr, spec: Mapping[str, Any], defs: Mapping[str, Any]) -> pl.Expr:
    valid = pl.lit(True)
    for keyword, value in spec.items():
        if keyword in _ANNOTATIONS:
            continue
        if keyword == "type":
            if value != "string":
                raise UnsupportedSchemaError(f"type {value!r} is not supported")
            valid = valid & column.is_not_null()
        elif keyword == "pattern":
            valid = valid & column.str.contains(value)
        elif keyword == "enum":
            valid = valid & column.is_in(value)
        elif keyword == "anyOf":
            options = [_rule(column, option, defs) for option in value]
            valid = valid & pl.any_horizontal(options)
        elif keyword == "$ref":
            valid = valid & _rule(column, _resolve(value, defs), defs)
        else:
            raise UnsupportedSchemaError(f"keyword {keyword!r} is not supported")
    return valid


def _resolve(reference: str, defs: Mapping[str, Any]) -> Mapping[str, Any]:
    prefix = "#/$defs/"
    if not reference.startswith(prefix) or reference[len(prefix) :] not in defs:
        raise UnsupportedSchemaError(f"cannot resolve $ref {reference!r}")
    return defs[reference[len(prefix) :]]


def find_violations(
    data: pl.LazyFrame, schema: Mapping[str, Any], *, examples: int = 3
) -> dict[str, tuple[int, list[str | None]]]:
    """
    Return the columns of ``data`` whose values break the schema.

    Parameters
    ----------
    data
        Rows to check; every schema column must be present.
    schema
        Source JSON Schema.
    examples
        Number of distinct invalid values to report per column.

    Returns
    -------
    dict
        ``{column: (invalid row count, example invalid values)}``; empty when
        every value is valid.
    """
    rules = column_rules(schema)
    counts = (
        data.select([(~rule).sum().alias(name) for name, rule in rules.items()])
        .collect(engine="streaming")
        .row(0, named=True)
    )
    violations = {}
    for name, count in counts.items():
        if count:
            bad = (
                data.filter(~rules[name])
                .select(pl.col(name).unique(maintain_order=True).head(examples))
                .collect(engine="streaming")
                .to_series()
                .to_list()
            )
            violations[name] = (count, bad)
    return violations


def format_violations(violations: Mapping[str, tuple[int, list[str | None]]]) -> str:
    """Return a one-line-per-column summary of ``find_violations`` output."""
    return "\n".join(
        f"{name}: {count} invalid, e.g. {examples!r}"
        for name, (count, examples) in violations.items()
    )
