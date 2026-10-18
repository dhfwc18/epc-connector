"""
Tests for ``epc_connector.cleaning.vocabulary``.
"""

import json
from pathlib import Path

import jsonschema
import pytest

from epc_connector.cleaning import descriptions, vocabulary
from epc_connector.cleaning.vocabulary import Element
from epc_connector.datasets import Origin

EW = Origin.ENGLAND_AND_WALES
SCOTLAND = Origin.SCOTLAND


class TestLoadVocabulary:
    def test_phrase_listed_twice_for_one_field_raises_value_error(
        self, vocabularies: Path
    ) -> None:
        path = vocabularies / "walls.vocabulary.json"
        document = json.loads(path.read_text())
        document["fields"]["construction"]["values"]["solid brick"].append(
            "cavity wall"
        )
        path.write_text(json.dumps(document))

        with pytest.raises(ValueError, match="'cavity wall' is listed twice"):
            vocabulary.load_vocabulary("walls", vocabularies)


class TestRepositoryVocabularies:
    @pytest.mark.parametrize("element", list(Element))
    def test_vocabulary_loads_and_matches_its_schema(self, element: Element) -> None:
        directory = vocabulary.DEFAULT_VOCABULARIES_DIR
        schema = json.loads((directory / "vocabulary.schema.json").read_text())
        document = json.loads((directory / f"{element}.vocabulary.json").read_text())

        jsonschema.validate(document, schema)
        assert vocabulary.load_vocabulary(element).element == element

    @pytest.mark.parametrize(
        ("raw", "element", "origin", "expected"),
        [
            (
                "Average thermal transmittance 0.30 W/m&#0178;K",
                "walls",
                EW,
                {"u_value": 0.3},
            ),
            (
                "Waliau ceudod, fel y\N{RIGHT SINGLE QUOTATION MARK}u hadeiladwyd, "
                "dim inswleiddio (rhagdybiaeth)",
                "walls",
                EW,
                {
                    "construction": "cavity wall",
                    "as_built": True,
                    "insulation": "none",
                    "insulation_assumed": True,
                },
            ),
            (
                "Pitched, 300+mm loft insulation",
                "roof",
                EW,
                {
                    "type": "pitched",
                    "insulation": "loft insulation",
                    "insulation_assumed": False,
                    "loft_insulation_thickness": 300,
                    "loft_insulation_or_more": True,
                },
            ),
            (
                "Boiler and radiators, |Bwyler a rheiddiaduron, "
                "|mains gas|nwy prif gyflenwad",
                "main-heating",
                EW,
                {"system": "boiler", "emitter": "radiators", "fuel": "mains gas"},
            ),
            (
                "Electric storage heaters",
                "main-heating",
                EW,
                {"system": "electric storage heaters", "fuel": "electric"},
            ),
            (
                "Community scheme utilising waste heat, mains gas",
                "main-heating",
                EW,
                {
                    "system": "community scheme",
                    "fuel": "waste heat",
                    "other_fuel": "mains gas",
                },
            ),
            (
                "Low energy lighting in all fixed outlets",
                "lighting",
                EW,
                {"low_energy_percent": 100.0},
            ),
            (
                "Single glazedDouble glazing",
                "windows",
                EW,
                {"glazing": "double", "coverage": "partial"},
            ),
            (
                "Programmer, room thermostat and TRVs",
                "heating-controls",
                EW,
                {"programmer": True, "room_thermostat": True, "trvs": True},
            ),
            ("Gas: mains gas", "main-fuel", EW, {"type": "mains gas"}),
        ],
    )
    def test_published_value_is_cleaned(
        self, raw: str, element: str, origin: Origin, expected: dict
    ) -> None:
        assert descriptions.clean_description(raw, element, origin).fields == expected

    @pytest.mark.parametrize(
        ("raw", "element", "origin", "reason"),
        [
            (
                "\N{LATIN CAPITAL LETTER A WITH CIRCUMFLEX}\N{SUPERSCRIPT TWO}K",
                "walls",
                EW,
                "truncated U-value",
            ),
            (
                "Pitched, 250 mm loft insulation",
                "walls",
                SCOTLAND,
                "roof description in the walls field",
            ),
        ],
    )
    def test_publisher_defect_is_discarded_with_its_reason(
        self, raw: str, element: str, origin: Origin, reason: str
    ) -> None:
        result = descriptions.clean_description(raw, element, origin)

        assert result.fields == {}
        assert result.discarded == reason
