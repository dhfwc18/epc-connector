"""
Tests for ``epc_connector.cleaning.descriptions``.
"""

import logging
from pathlib import Path

import polars as pl
import pytest

from epc_connector.cleaning import descriptions
from epc_connector.datasets import Origin

EW = Origin.ENGLAND_AND_WALES
SCOTLAND = Origin.SCOTLAND


def clean(
    raw: str, element: str, directory: Path, origin: Origin = EW
) -> descriptions.CleanedDescription:
    return descriptions.clean_description(raw, element, origin, directory)


class TestCleanDescription:
    def test_phrases_give_field_values(self, vocabularies: Path) -> None:
        result = clean("Cavity wall, no insulation (assumed)", "walls", vocabularies)

        assert result.fields == {
            "construction": "cavity wall",
            "insulation": "none",
            "insulation_assumed": True,
        }
        assert result.parts == 1

    def test_number_is_read_from_a_hash_phrase(self, vocabularies: Path) -> None:
        assert clean("U -0.25 W/m2K", "walls", vocabularies).fields == {
            "u_value": -0.25
        }

    def test_welsh_phrase_listed_beside_english_gives_the_same_value(
        self, vocabularies: Path
    ) -> None:
        assert clean("Waliau ceudod", "walls", vocabularies).fields == {
            "construction": "cavity wall"
        }

    def test_ignored_phrase_gives_nothing(self, vocabularies: Path) -> None:
        assert clean("Solid brick, as built", "walls", vocabularies).fields == {
            "construction": "solid brick"
        }

    def test_discarded_description_records_the_reason(self, vocabularies: Path) -> None:
        result = clean("SAP05:Walls", "walls", vocabularies)

        assert result.fields == {}
        assert result.discarded == "placeholder text"

    def test_whole_element_listed_is_not_split(self, vocabularies: Path) -> None:
        assert clean("Solid brick, filled cavity", "walls", vocabularies).fields == {
            "construction": "solid brick"
        }

    def test_unknown_phrase_raises(self, vocabularies: Path) -> None:
        with pytest.raises(descriptions.UnknownPhraseError, match="'thatch'"):
            clean("Cavity wall, thatch", "walls", vocabularies)

    def test_conflicting_phrases_raise(self, vocabularies: Path) -> None:
        with pytest.raises(descriptions.PhraseConflictError, match="construction"):
            clean("Cavity wall, solid brick", "walls", vocabularies)

    def test_first_usable_scottish_part_is_the_main_element(
        self, vocabularies: Path
    ) -> None:
        result = clean(
            "SAP05:Walls | Cavity wall | Solid brick", "walls", vocabularies, SCOTLAND
        )

        assert result.fields == {"construction": "cavity wall"}
        assert result.parts == 3
        assert result.discarded is None

    def test_first_system_is_kept_and_systems_are_counted(
        self, vocabularies: Path
    ) -> None:
        result = clean("Boiler, mains gas, boiler, oil", "main-heating", vocabularies)

        assert result.fields == {"system": "boiler", "fuel": "mains gas"}
        assert result.systems == 2

    def test_empty_slot_starts_an_unspecified_system(self, vocabularies: Path) -> None:
        result = clean("Boiler, mains gas, , oil", "main-heating", vocabularies)

        assert result.systems == 2

    def test_second_fuel_goes_to_the_second_value_field(
        self, vocabularies: Path
    ) -> None:
        result = clean("Boiler, oil, mains gas", "main-heating", vocabularies)

        assert result.fields == {
            "system": "boiler",
            "fuel": "oil",
            "other_fuel": "mains gas",
        }


class TestCleanValues:
    def test_one_typed_row_per_distinct_value(self, vocabularies: Path) -> None:
        table = descriptions.clean_values(
            ["U 0.3 W/m2K", None, "Cavity wall", "Cavity wall"],
            "walls",
            EW,
            vocabularies_dir=vocabularies,
        )

        assert table["raw"].to_list() == ["U 0.3 W/m2K", "Cavity wall"]
        assert table["walls_u_value"].to_list() == [0.3, None]
        assert table.schema["walls_insulation_assumed"] == pl.Boolean
        assert table["walls_parts"].to_list() == [1, 1]

    def test_strict_mode_lists_every_unknown_phrase(self, vocabularies: Path) -> None:
        with pytest.raises(descriptions.UnknownPhraseError) as error:
            descriptions.clean_values(
                ["Thatch", "Cob"], "walls", EW, vocabularies_dir=vocabularies
            )

        assert "2 descriptions not covered" in str(error.value)

    def test_lenient_mode_marks_unknown_values(self, vocabularies: Path) -> None:
        table = descriptions.clean_values(
            ["Thatch"], "walls", EW, strict=False, vocabularies_dir=vocabularies
        )

        assert table["walls_discarded"].to_list() == ["not in vocabulary"]

    def test_lenient_mode_warns_once_and_lists_values_in_debug(
        self, vocabularies: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            descriptions.clean_values(
                ["Thatch", "Cob"],
                "walls",
                EW,
                strict=False,
                vocabularies_dir=vocabularies,
            )

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        details = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert [r.getMessage() for r in warnings] == [
            "2 walls descriptions are not in the vocabulary; marked as discarded"
        ]
        assert len(details) == 2
