"""
Tests for ``epc_connector.validation``.
"""

from pathlib import Path

import polars as pl
import pytest

from epc_connector import validation
from epc_connector.validation import UnsupportedSchemaError
from tests.fakes import abc_schema

SOURCE_SCHEMAS = sorted((Path(__file__).parents[1] / "schemas/source").rglob("*.json"))


class TestCheckHeader:
    def test_matching_header_passes(self) -> None:
        validation.check_header(["A", "B", "C"], abc_schema())

    def test_missing_and_unexpected_columns_raise_value_error(self) -> None:
        with pytest.raises(ValueError, match=r"missing \['C'\], unexpected \['D'\]"):
            validation.check_header(["A", "B", "D"], abc_schema())

    def test_reordered_columns_raise_value_error(self) -> None:
        with pytest.raises(ValueError, match="different order"):
            validation.check_header(["B", "A", "C"], abc_schema())


class TestColumnRules:
    @pytest.mark.parametrize(
        "schema_path", SOURCE_SCHEMAS, ids=lambda p: f"{p.parent.name}/{p.name}"
    )
    def test_every_source_schema_translates(self, schema_path: Path) -> None:
        schema = validation.load_schema(schema_path)

        rules = validation.column_rules(schema)

        assert list(rules) == validation.schema_columns(schema)

    def test_unsupported_keyword_raises_unsupported_schema_error(self) -> None:
        schema = abc_schema()
        schema["properties"]["B"]["minLength"] = 1

        with pytest.raises(UnsupportedSchemaError, match="minLength"):
            validation.column_rules(schema)

    def test_unresolvable_ref_raises_unsupported_schema_error(self) -> None:
        schema = abc_schema()
        schema["properties"]["A"]["anyOf"][0] = {"$ref": "#/$defs/absent"}

        with pytest.raises(UnsupportedSchemaError, match="absent"):
            validation.column_rules(schema)


class TestFindViolations:
    def test_valid_data_has_no_violations(self) -> None:
        data = pl.LazyFrame(
            {"A": ["1", "", "Data removed"], "B": ["", "b", "c"], "C": ["x", "", "z"]}
        )

        assert validation.find_violations(data, abc_schema()) == {}

    def test_pattern_and_enum_violations_are_counted_with_examples(self) -> None:
        data = pl.LazyFrame(
            {"A": ["1", "one", "one", "2.5"], "B": ["b"] * 4, "C": ["x", "w", "x", "x"]}
        )

        violations = validation.find_violations(data, abc_schema())

        assert violations == {"A": (3, ["one", "2.5"]), "C": (1, ["w"])}

    def test_null_in_string_column_is_a_violation(self) -> None:
        data = pl.LazyFrame(
            {"A": ["1"], "B": [None], "C": ["x"]},
            schema={"A": pl.String, "B": pl.String, "C": pl.String},
        )

        assert validation.find_violations(data, abc_schema()) == {"B": (1, [None])}


class TestFormatViolations:
    def test_one_line_per_column(self) -> None:
        text = validation.format_violations({"A": (3, ["one"]), "C": (1, ["w"])})

        assert text == "A: 3 invalid, e.g. ['one']\nC: 1 invalid, e.g. ['w']"
