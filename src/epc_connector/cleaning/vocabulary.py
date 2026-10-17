"""
Read the vocabulary files in ``schemas/vocabularies/`` into phrase lookups.
"""

import functools
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import polars as pl

from epc_connector.validation import DEFAULT_SCHEMAS_DIR

DEFAULT_VOCABULARIES_DIR: Final = DEFAULT_SCHEMAS_DIR / "vocabularies"
NUMBER_PLACEHOLDER: Final = "#"

_DTYPES: Final[dict[str, Any]] = {
    "string": pl.String,
    "boolean": pl.Boolean,
    "integer": pl.Int64,
    "number": pl.Float64,
}


class Element(StrEnum):
    """
    Described building element; each has its own vocabulary file.

    Attributes
    ----------
    WALLS, ROOF, FLOOR, WINDOWS
        Building fabric.
    GLAZED_TYPE
        Glazing type and installation period.
    LIGHTING
        Low-energy lighting share or lighting efficiency.
    MAIN_HEATING, HEATING_CONTROLS, HOT_WATER, SECONDARY_HEATING
        Heating and hot water systems.
    MAIN_FUEL
        Main heating fuel.
    """

    WALLS = "walls"
    ROOF = "roof"
    FLOOR = "floor"
    WINDOWS = "windows"
    GLAZED_TYPE = "glazed-type"
    LIGHTING = "lighting"
    MAIN_HEATING = "main-heating"
    HEATING_CONTROLS = "heating-controls"
    HOT_WATER = "hot-water"
    SECONDARY_HEATING = "secondary-heating"
    MAIN_FUEL = "main-fuel"


@dataclass(frozen=True)
class Vocabulary:
    """
    One element's vocabulary, inverted for lookup.

    Attributes
    ----------
    element
        Element the vocabulary describes.
    prefix
        Output column prefix; field ``construction`` of prefix ``walls`` is
        written to column ``walls_construction``.
    split_phrases
        Whether elements are split on commas into phrases.
    count_field
        Field that starts a new system when given again (main heating lists
        several systems in one description), or None.
    second_value_fields
        Field -> field that takes a second, different value (``fuel`` ->
        ``other_fuel``) instead of raising a conflict.
    field_types
        Type of each field: ``string``, ``boolean``, ``integer`` or
        ``number``.
    lookup
        Phrase -> the field values it gives. ``NUMBER_PLACEHOLDER`` as a value
        means "the number in the phrase" (listed under ``number_from``).
    discard
        Phrase -> reason the whole element is dropped.
    ignore
        Phrase -> reason the phrase is skipped.
    """

    element: Element
    prefix: str
    split_phrases: bool
    count_field: str | None
    second_value_fields: Mapping[str, str]
    field_types: Mapping[str, str]
    lookup: Mapping[str, Mapping[str, Any]]
    discard: Mapping[str, str]
    ignore: Mapping[str, str]

    @property
    def columns(self) -> dict[str, Any]:
        """Output column names and Polars types, in order."""
        columns = {
            f"{self.prefix}_{name}": _DTYPES[kind]
            for name, kind in self.field_types.items()
        }
        columns[f"{self.prefix}_parts"] = pl.Int32
        if self.count_field:
            columns[f"{self.prefix}_systems"] = pl.Int32
        columns[f"{self.prefix}_discarded"] = pl.String
        return columns


@functools.cache
def load_vocabulary(
    element: Element | str, vocabularies_dir: Path = DEFAULT_VOCABULARIES_DIR
) -> Vocabulary:
    """
    Return an element's vocabulary, inverted to phrase -> field values.

    Raises
    ------
    ValueError
        If a phrase gives one field two different values.
    """
    element = Element(element)
    path = Path(vocabularies_dir) / f"{element}.vocabulary.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    fields = document["fields"]
    lookup: dict[str, dict[str, Any]] = {}
    for name, spec in fields.items():
        pairs = [
            (_typed(value, spec["type"]), phrase)
            for value, listed in spec.get("values", {}).items()
            for phrase in listed
        ]
        pairs += [
            (NUMBER_PLACEHOLDER, phrase) for phrase in spec.get("number_from", [])
        ]
        for value, phrase in pairs:
            given = lookup.setdefault(phrase, {})
            if name in given:
                raise ValueError(f"{path.name}: {phrase!r} is listed twice for {name}")
            given[name] = value
    return Vocabulary(
        element=element,
        prefix=document["prefix"],
        split_phrases=document.get("split_phrases", True),
        count_field=document.get("count_systems_by"),
        second_value_fields={
            name: spec["second_value_field"]
            for name, spec in fields.items()
            if "second_value_field" in spec
        },
        field_types={name: spec["type"] for name, spec in fields.items()},
        lookup=lookup,
        discard=_by_phrase(document.get("discard", {})),
        ignore=_by_phrase(document.get("ignore", {})),
    )


def parse_number(text: str, kind: str) -> int | float:
    """Return ``text`` as an ``integer`` or ``number`` field value."""
    return int(float(text)) if kind == "integer" else float(text)


def _typed(value: str, kind: str) -> str | bool | int | float:
    """Convert a value key from the JSON file to the field's type."""
    if kind == "boolean":
        return {"true": True, "false": False}[value]
    if kind in ("integer", "number"):
        return parse_number(value, kind)
    return value


def _by_phrase(reasons: Mapping[str, list[str]]) -> dict[str, str]:
    """Invert ``{reason: [phrases]}`` to ``{phrase: reason}``."""
    return {phrase: reason for reason, listed in reasons.items() for phrase in listed}
