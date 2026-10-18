"""
Tests for ``epc_connector.canonical.mapping``.
"""

import datetime as dt
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from epc_connector.canonical import mapping
from epc_connector.cleaning import Element, load_vocabulary


def compile_mapping(tmp_path: Path, fields: dict[str, Any]) -> mapping.CompiledMapping:
    """Write and load a mapping file of ``fields`` (canonical field names)."""
    document = {
        "source": "source/test.schema.json",
        "target": "canonical/gb-domestic.schema.json",
        "null_values": ["", "N/A"],
        "fields": fields,
    }
    (tmp_path / "test.mapping.json").write_text(json.dumps(document))
    return mapping.load_mapping("test", tmp_path)


def map_rows(
    tmp_path: Path, fields: dict[str, Any], rows: dict[str, list[str]]
) -> dict[str, list[Any]]:
    """Check and map ``rows`` with a mapping file of ``fields``."""
    compiled = compile_mapping(tmp_path, fields)
    source = pl.LazyFrame(rows)
    mapping.check_values(source, compiled)
    frame = mapping.apply_mapping(source, compiled, list(fields)).collect()
    return frame.to_dict(as_series=False)


class TestLoadMapping:
    def test_values_are_trimmed_and_null_values_become_null(
        self, tmp_path: Path
    ) -> None:
        result = map_rows(
            tmp_path, {"postcode": {"from": "pc"}}, {"pc": [" AB1 2CD ", "N/A"]}
        )

        assert result == {"postcode": ["AB1 2CD", None]}

    def test_constant_is_cast_to_the_canonical_type(self, tmp_path: Path) -> None:
        result = map_rows(tmp_path, {"superseded": {"const": None}}, {"x": ["a"]})

        assert result == {"superseded": [None]}

    def test_first_part_skips_empty_and_listed_parts(self, tmp_path: Path) -> None:
        rule = {
            "from": "eff",
            "steps": [
                {"op": "first_part", "separator": "|", "skip": ["N/A"]},
                {"op": "lower"},
            ],
        }
        rows = {"eff": ["Good", " | N/A | Poor", "N/A | N/A"]}

        result = map_rows(tmp_path, {"walls_energy_efficiency": rule}, rows)

        assert result == {"walls_energy_efficiency": ["good", "poor", None]}

    @pytest.mark.parametrize(
        ("unmapped", "expected"), [("keep", ["owner", "x"]), ("null", ["owner", None])]
    )
    def test_value_map_keeps_or_nulls_unmapped_values(
        self, tmp_path: Path, unmapped: str, expected: list
    ) -> None:
        rule = {
            "from": "t",
            "steps": [{"op": "map", "values": {"o": "owner"}, "unmapped": unmapped}],
        }

        result = map_rows(tmp_path, {"tenure": rule}, {"t": ["o", "x"]})

        assert result == {"tenure": expected}

    def test_unmapped_values_are_all_reported_before_writing(
        self, tmp_path: Path
    ) -> None:
        rule = {
            "from": "band",
            "steps": [
                {"op": "map", "values": {"1919-1929": 1919}, "unmapped": "error"}
            ],
        }

        with pytest.raises(mapping.MappingError, match=r"\['1700s', 'unknown'\]"):
            map_rows(
                tmp_path,
                {"construction_year_min": rule},
                {"band": ["1919-1929", "1700s", "unknown", ""]},
            )

    def test_mapped_numbers_become_the_canonical_integer_type(
        self, tmp_path: Path
    ) -> None:
        rule = {
            "from": "band",
            "steps": [
                {"op": "map", "values": {"1919-1929": 1919}, "unmapped": "error"}
            ],
        }

        result = map_rows(
            tmp_path, {"construction_year_min": rule}, {"band": ["1919-1929"]}
        )

        assert result == {"construction_year_min": [1919]}

    def test_integer_cast_accepts_whole_numbers_written_with_decimals(
        self, tmp_path: Path
    ) -> None:
        rule = {"from": "u", "steps": [{"op": "cast", "to": "integer"}]}

        result = map_rows(tmp_path, {"uprn": rule}, {"u": ["100023", "72.0"]})

        assert result == {"uprn": [100023, 72]}

    def test_bad_number_stops_the_write(self, tmp_path: Path) -> None:
        rule = {"from": "u", "steps": [{"op": "cast", "to": "integer"}]}

        with pytest.raises(pl.exceptions.InvalidOperationError):
            map_rows(tmp_path, {"uprn": rule}, {"u": ["72.5"]})

    def test_dates_and_date_times_are_parsed_with_their_format(
        self, tmp_path: Path
    ) -> None:
        fields = {
            "lodgement_date": {
                "from": "d",
                "steps": [{"op": "cast", "to": "date", "format": "%Y-%m-%d"}],
            },
            "lodged_at": {
                "from": "t",
                "steps": [
                    {"op": "cast", "to": "datetime", "format": "%d/%m/%Y %H:%M:%S"}
                ],
            },
        }

        result = map_rows(
            tmp_path, fields, {"d": ["2024-02-29"], "t": ["08/12/2008 11:22:22"]}
        )

        assert result == {
            "lodgement_date": [dt.date(2024, 2, 29)],
            "lodged_at": [dt.datetime(2008, 12, 8, 11, 22, 22)],
        }

    def test_boolean_reports_values_in_neither_list(self, tmp_path: Path) -> None:
        rule = {
            "from": "f",
            "steps": [{"op": "boolean", "true": ["Y"], "false": ["N"]}],
        }

        assert map_rows(
            tmp_path, {"has_solar_water_heating": rule}, {"f": ["Y", "N", ""]}
        ) == {"has_solar_water_heating": [True, False, None]}
        with pytest.raises(mapping.MappingError, match=r"\['maybe'\]"):
            map_rows(tmp_path, {"has_solar_water_heating": rule}, {"f": ["maybe"]})

    def test_numeric_steps(self, tmp_path: Path) -> None:
        fields = {
            "heating_cost_current": {
                "from": "cost",
                "steps": [
                    {"op": "cast", "to": "number"},
                    {"op": "divide", "by": 3},
                    {"op": "round", "digits": 2},
                ],
            },
            "total_floor_area": {
                "from": "area",
                "steps": [
                    {"op": "cast", "to": "number"},
                    {"op": "null_if", "values": [0]},
                ],
            },
            "has_solar_pv": {
                "from": "pv",
                "steps": [
                    {"op": "regex_extract", "pattern": "Area: *([0-9.]+)", "group": 1},
                    {"op": "cast", "to": "number"},
                    {"op": "greater_than", "value": 0},
                ],
            },
        }
        rows = {
            "cost": ["100", ".30"],
            "area": ["0", "11."],
            "pv": ["Roof Area: 25%", "Roof Area: 0%"],
        }

        result = map_rows(tmp_path, fields, rows)

        assert result == {
            "heating_cost_current": [33.33, 0.1],
            "total_floor_area": [None, 11.0],
            "has_solar_pv": [True, False],
        }

    def test_strip_prefix(self, tmp_path: Path) -> None:
        rule = {
            "from": "band",
            "steps": [{"op": "strip_prefix", "prefix": "England and Wales: "}],
        }

        result = map_rows(
            tmp_path,
            {"construction_age_band": rule},
            {"band": ["England and Wales: 1930-1949"]},
        )

        assert result == {"construction_age_band": ["1930-1949"]}


class TestApplyMapping:
    def test_other_canonical_columns_are_taken_from_the_source_and_cast(
        self, tmp_path: Path
    ) -> None:
        compiled = compile_mapping(tmp_path, {"postcode": {"from": "pc"}})
        source = pl.LazyFrame(
            {"pc": ["AB1"], "walls_parts": [1]},
            schema={"pc": pl.String, "walls_parts": pl.Int32},
        )

        frame = mapping.apply_mapping(source, compiled, ["postcode", "walls_parts"])

        assert frame.collect().to_dict(as_series=False) == {
            "postcode": ["AB1"],
            "walls_parts": [1],
        }
        assert frame.collect_schema() == {
            "postcode": pl.String,
            "walls_parts": pl.Int64,
        }


class TestRepositoryMappings:
    @pytest.mark.parametrize(
        "name", ["england-and-wales-domestic", "scotland-domestic"]
    )
    def test_mapping_and_cleaned_columns_give_every_canonical_field_once(
        self, name: str
    ) -> None:
        mapped = set(mapping.load_mapping(name).expressions)
        cleaned = [c for e in Element for c in load_vocabulary(e).columns]

        assert not mapped & set(cleaned)
        assert mapped | set(cleaned) == set(mapping.canonical_dtypes())
        assert len(cleaned) == len(set(cleaned))

    def test_canonical_cleaned_fields_match_the_vocabularies(self) -> None:
        schema = json.loads(mapping.CANONICAL_SCHEMA_PATH.read_text())
        dtypes = mapping.canonical_dtypes()
        for element in Element:
            vocabulary = load_vocabulary(element)
            for column, dtype in vocabulary.columns.items():
                expected = pl.Int64 if dtype == pl.Int32 else dtype
                assert dtypes[column] == expected, column
            path = (
                mapping.DEFAULT_SCHEMAS_DIR
                / "vocabularies"
                / f"{element}.vocabulary.json"
            )
            fields = json.loads(path.read_text())["fields"]
            for name, spec in fields.items():
                if spec["type"] == "string" and spec.get("values"):
                    enum = schema["properties"][f"{vocabulary.prefix}_{name}"]["enum"]
                    assert enum == [*spec["values"], None], name
