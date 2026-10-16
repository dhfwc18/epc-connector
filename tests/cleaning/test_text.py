"""
Tests for ``epc_connector.cleaning.text``.
"""

import pytest

from epc_connector.cleaning.text import Nation, normalise, split_elements

EW = Nation.ENGLAND_AND_WALES
SCOTLAND = Nation.SCOTLAND


class TestSplitElements:
    def test_plain_value_is_one_element(self) -> None:
        assert split_elements(" Cavity wall ", EW) == ["Cavity wall"]

    def test_bilingual_pair_keeps_the_english_part(self) -> None:
        raw = "Solid, no insulation (assumed)|Solet, dim inswleiddio (rhagdybiaeth)"

        assert split_elements(raw, EW) == ["Solid, no insulation (assumed)"]

    def test_interleaved_bilingual_value_joins_the_english_parts(self) -> None:
        raw = "Boiler and radiators, |Bwyler a rheiddiaduron, |mains gas|nwy"

        assert split_elements(raw, EW) == ["Boiler and radiators, mains gas"]

    def test_scottish_building_parts_keep_their_positions(self) -> None:
        raw = " | Cavity wall, filled cavity | "

        assert split_elements(raw, SCOTLAND) == [
            "",
            "Cavity wall, filled cavity",
            "",
        ]


class TestNormalise:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("Average 0.30 W/m&#0178;K", "average 0.30 w/m\N{SUPERSCRIPT TWO}k"),
            ("Boiler &amp; underfloor", "boiler and underfloor"),
            ("Cavity wall" + "?" * 52 + "as built", "cavity wall, as built"),
            ("fel y\N{RIGHT SINGLE QUOTATION MARK}u", "fel y'u"),
            ("Description: Single glazed", "single glazed"),
            ("Pitched, 300+mm loft insulation", "pitched, 300+ mm loft insulation"),
            ("Pitched, no insulation(assumed)", "pitched, no insulation (assumed)"),
            ("To unheated space,insulated ", "to unheated space, insulated"),
            (
                "2207 Time and temperature zone control",
                "time and temperature zone control",
            ),
            ("Boiler and radiators, ", "boiler and radiators"),
        ],
    )
    def test_value_is_normalised(self, raw: str, expected: str) -> None:
        assert normalise(raw) == expected
