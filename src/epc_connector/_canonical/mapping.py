"""
Map source rows to the canonical record with ``schemas/mappings/`` files.

Each canonical field is a constant or one source column passed through steps
(``mapping.schema.json`` defines them). Every step becomes a Polars expression
that streams, so a whole file is mapped in one pass. Value maps are built from
``when/then`` chains: ``replace`` loads the whole column into memory.

A step that meets a value it cannot handle must be an error. Casts are strict,
so a bad value stops the write. Values that a ``map`` (``unmapped: error``) or
``boolean`` step does not list are found before the write, by ``check_values``
on the distinct source values, and reported together.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import polars as pl

from epc_connector._errors import EpcConnectorError
from epc_connector._validation import DEFAULT_SCHEMAS_DIR

DEFAULT_MAPPINGS_DIR: Final = DEFAULT_SCHEMAS_DIR / "mappings"
"""Repository directory holding the mapping files."""
CANONICAL_SCHEMA_PATH: Final = (
    DEFAULT_SCHEMAS_DIR / "canonical" / "gb-domestic.schema.json"
)
"""JSON Schema of the canonical GB domestic record."""


class MappingError(EpcConnectorError, ValueError):
    """Source values that a mapping step does not handle."""


@dataclass(frozen=True)
class ValueCheck:
    """
    Values a step accepts, checked on the distinct source values.

    Attributes
    ----------
    field
        Canonical field being built.
    column
        Source column.
    expression
        The field's expression up to (not including) the checked step.
    allowed
        Values the step handles; anything else, except null, is an error.
    """

    field: str
    column: str
    expression: pl.Expr
    allowed: frozenset[str]


@dataclass
class CompiledMapping:
    """
    A mapping file translated to Polars.

    Attributes
    ----------
    expressions
        Canonical field -> expression over the source columns.
    checks
        Value checks to run on the distinct source values before writing.
    """

    expressions: dict[str, pl.Expr] = field(default_factory=dict)
    checks: list[ValueCheck] = field(default_factory=list)


def load_mapping(
    name: str, mappings_dir: Path = DEFAULT_MAPPINGS_DIR
) -> CompiledMapping:
    """
    Return ``<mappings_dir>/<name>.mapping.json`` translated to Polars.

    Each expression ends with a cast to the field's type in the canonical
    schema (``canonical_dtypes``).
    """
    path = Path(mappings_dir) / f"{name}.mapping.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    dtypes = canonical_dtypes()
    mapping = CompiledMapping()
    for name_, rule in document["fields"].items():
        if "const" in rule:
            expression = pl.lit(rule["const"])
        else:
            expression = _field_expression(
                name_, rule, document["null_values"], mapping.checks
            )
        mapping.expressions[name_] = expression.cast(dtypes[name_], strict=True)
    return mapping


def canonical_dtypes(schema_path: Path = CANONICAL_SCHEMA_PATH) -> dict[str, Any]:
    """Return the Polars type of every canonical field, in schema order."""
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    return {
        name: _dtype(spec, schema["$defs"])
        for name, spec in schema["properties"].items()
    }


def check_values(source: pl.LazyFrame, mapping: CompiledMapping) -> None:
    """
    Raise if a checked step meets a source value it does not list.

    Reads the distinct values of the checked columns in one streaming pass.

    Raises
    ------
    MappingError
        Listing every field, step and unhandled value.
    """
    columns = sorted({check.column for check in mapping.checks})
    if not columns:
        return
    distinct = source.select(pl.col(c).unique().implode() for c in columns).collect(
        engine="streaming"
    )
    problems = []
    for check in mapping.checks:
        values = pl.DataFrame({check.column: distinct[check.column][0]})
        found = values.select(check.expression.alias("value"))["value"]
        unhandled = sorted(
            {value for value in found.drop_nulls() if value not in check.allowed}
        )
        if unhandled:
            problems.append(f"{check.field} ({check.column}): {unhandled}")
    if problems:
        raise MappingError("values not handled by the mapping:\n" + "\n".join(problems))


def apply_mapping(
    source: pl.LazyFrame, mapping: CompiledMapping, columns: Sequence[str]
) -> pl.LazyFrame:
    """
    Return the canonical columns, in order, from a source frame.

    A column with a mapping expression is computed from it; any other column
    (e.g. the cleaned description columns) is taken from ``source`` as is,
    cast to its canonical type.
    """
    dtypes = canonical_dtypes()
    return source.select(
        mapping.expressions.get(name, pl.col(name).cast(dtypes[name])).alias(name)
        for name in columns
    )


def _field_expression(
    name: str, rule: Mapping[str, Any], null_values: list[str], checks: list[ValueCheck]
) -> pl.Expr:
    """Build one field: trim, null values, then each step in order."""
    column = rule["from"]
    trimmed = pl.col(column).str.strip_chars()
    expression = pl.when(trimmed.is_in(null_values)).then(None).otherwise(trimmed)
    for step in rule.get("steps", []):
        allowed = _allowed_values(step)
        if allowed is not None:
            checks.append(ValueCheck(name, column, expression, allowed))
        expression = _step(expression, step)
    return expression


def _allowed_values(step: Mapping[str, Any]) -> frozenset[str] | None:
    """Values a ``map`` (unmapped: error) or ``boolean`` step must be given."""
    if step["op"] == "map" and step["unmapped"] == "error":
        return frozenset(step["values"])
    if step["op"] == "boolean":
        return frozenset(step["true"]) | frozenset(step["false"])
    return None


def _step(expression: pl.Expr, step: Mapping[str, Any]) -> pl.Expr:
    """Translate one mapping step (see ``mapping.schema.json``)."""
    match step["op"]:
        case "first_part":
            part = pl.element().str.strip_chars()
            keep = (part != "") & ~part.is_in(step.get("skip", []))
            return (
                expression.str.split(step["separator"])
                .list.eval(part.filter(keep))
                .list.first()
            )
        case "lower":
            return expression.str.to_lowercase()
        case "strip_prefix":
            return expression.str.strip_prefix(step["prefix"])
        case "map":
            return _value_map(expression, step["values"], step["unmapped"])
        case "cast":
            return _cast(expression, step["to"], step.get("format"))
        case "boolean":
            return (
                pl.when(expression.is_in(step["true"]))
                .then(True)
                .when(expression.is_in(step["false"]))
                .then(False)
            )
        case "divide":
            return expression / step["by"]
        case "round":
            return expression.round(step["digits"])
        case "greater_than":
            return expression > step["value"]
        case "null_if":  # == rather than is_in: values may be numbers, e.g. 0
            matches = pl.any_horizontal(expression == value for value in step["values"])
            return pl.when(matches).then(None).otherwise(expression)
        case "regex_extract":
            return expression.str.extract(step["pattern"], step["group"])
    raise ValueError(f"unknown mapping step {step['op']!r}")


def _value_map(
    expression: pl.Expr, values: Mapping[str, Any], unmapped: str
) -> pl.Expr:
    """
    Map whole values with a ``when/then`` chain; results are strings.

    Mapped numbers (e.g. construction years) stay text until the field's
    final cast, so mapped and kept values share one type.
    """
    chain: Any = pl
    for key, value in values.items():
        chain = chain.when(expression == key).then(
            pl.lit(None, pl.String) if value is None else pl.lit(str(value))
        )
    return chain.otherwise(expression if unmapped == "keep" else None)


def _cast(expression: pl.Expr, to: str, format_: str | None) -> pl.Expr:
    match to:
        case "integer":  # a whole number written as "72.0" is accepted
            return expression.str.replace(r"\.0+$", "").cast(pl.Int64, strict=True)
        case "number":
            return expression.cast(pl.Float64, strict=True)
        case "date":
            return expression.str.to_date(format_ or "%Y-%m-%d", strict=True)
        case "datetime":
            return expression.str.to_datetime(format_, strict=True)
    raise ValueError(f"unknown cast target {to!r}")


def _dtype(spec: Mapping[str, Any], defs: Mapping[str, Any]) -> Any:
    """Polars type of a canonical schema property."""
    if "$ref" in spec:
        spec = defs[spec["$ref"].removeprefix("#/$defs/")] | spec
    if spec.get("format") == "date":
        return pl.Date
    if spec.get("format") == "date-time":
        return pl.Datetime("us")
    kinds = spec["type"] if isinstance(spec["type"], list) else [spec["type"]]
    kind = next(k for k in kinds if k != "null")
    return {
        "string": pl.String,
        "integer": pl.Int64,
        "number": pl.Float64,
        "boolean": pl.Boolean,
    }[kind]
