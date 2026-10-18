"""
Clean descriptions with a vocabulary: one value, or a table of distinct values.
"""

import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import polars as pl

from epc_connector.cleaning.text import normalise, phrases, split_elements
from epc_connector.cleaning.vocabulary import (
    DEFAULT_VOCABULARIES_DIR,
    NUMBER_PLACEHOLDER,
    Element,
    Vocabulary,
    load_vocabulary,
    parse_number,
)
from epc_connector.datasets import Origin
from epc_connector.errors import EpcConnectorError

logger = logging.getLogger(__name__)

_NUMBER: Final = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?(?![\w.])")


class UnknownPhraseError(EpcConnectorError, ValueError):
    """Phrases that no vocabulary entry lists."""


class PhraseConflictError(EpcConnectorError, ValueError):
    """Two phrases of one element give a field different values."""


@dataclass
class CleanedDescription:
    """
    Fields of one raw description's main element.

    Attributes
    ----------
    fields
        Field values of the main element: the first building part that is not
        discarded and, where several systems are listed, the first system.
    parts
        Number of building parts with a description.
    systems
        Number of systems listed in the main part (``count_field`` only).
    discarded
        Reason, if every part was discarded.
    """

    fields: dict[str, Any] = field(default_factory=dict)
    parts: int = 0
    systems: int = 0
    discarded: str | None = None


def clean_description(
    raw: str,
    element: Element | str,
    origin: Origin | str,
    vocabularies_dir: Path = DEFAULT_VOCABULARIES_DIR,
) -> CleanedDescription:
    """
    Return the fields of one raw description's main element.

    Raises
    ------
    UnknownPhraseError
        If a phrase is not listed in the vocabulary.
    PhraseConflictError
        If two phrases give one field different values.
    """
    vocabulary = load_vocabulary(element, vocabularies_dir)
    cleaned = CleanedDescription()
    for part in split_elements(raw, origin):
        text = normalise(part)
        if not text:
            continue
        cleaned.parts += 1
        systems: list[dict[str, Any]] = [{}]
        reason = None
        for phrase in _element_phrases(text, vocabulary):
            reason = vocabulary.discard.get(phrase)
            if reason is not None:
                break
            if phrase in vocabulary.ignore:
                continue
            for name, value in _lookup(phrase, vocabulary, raw).items():
                if name == vocabulary.count_field and name in systems[-1]:
                    systems.append({})
                _give(systems[-1], name, value, vocabulary, raw)
        if reason is not None:
            cleaned.discarded = cleaned.discarded or reason
        elif not cleaned.fields and cleaned.systems == 0:
            cleaned.fields = systems[0]
            cleaned.systems = len(systems)
            cleaned.discarded = None
    return cleaned


def clean_values(
    values: Iterable[str | None],
    element: Element | str,
    origin: Origin | str,
    *,
    strict: bool = True,
    vocabularies_dir: Path = DEFAULT_VOCABULARIES_DIR,
) -> pl.DataFrame:
    """
    Return a lookup table from raw descriptions to their output columns.

    One row per distinct non-null value: ``raw`` plus ``Vocabulary.columns``.
    With ``strict``, unknown phrases and conflicts raise one error listing all
    of them; otherwise those values get ``<prefix>_discarded`` set to
    ``"not in vocabulary"`` and are logged as a warning.

    Raises
    ------
    UnknownPhraseError
        In strict mode, if any value has a phrase the vocabulary does not
        list, or two phrases that conflict.
    """
    vocabulary = load_vocabulary(element, vocabularies_dir)
    prefix = vocabulary.prefix
    rows, problems = [], []
    for raw in dict.fromkeys(values):
        if raw is None:
            continue
        try:
            cleaned = clean_description(raw, element, origin, vocabularies_dir)
        except (UnknownPhraseError, PhraseConflictError) as error:
            problems.append(str(error))
            cleaned = CleanedDescription(discarded="not in vocabulary")
        row = {f"{prefix}_{name}": value for name, value in cleaned.fields.items()}
        row |= {"raw": raw, f"{prefix}_parts": cleaned.parts}
        row[f"{prefix}_discarded"] = cleaned.discarded
        if vocabulary.count_field:
            row[f"{prefix}_systems"] = cleaned.systems
        rows.append(row)
    if problems and strict:
        raise UnknownPhraseError(
            f"{len(problems)} descriptions not covered:\n" + "\n".join(problems)
        )
    if problems:
        logger.warning(
            "%d %s descriptions are not in the vocabulary; marked as discarded",
            len(problems),
            element,
        )
    for problem in problems:
        logger.debug("Not in the %s vocabulary: %s", element, problem)
    return pl.DataFrame(rows, schema={"raw": pl.String, **vocabulary.columns})


def _element_phrases(text: str, vocabulary: Vocabulary) -> list[str]:
    """
    Return the phrases of a normalised element.

    A whole element that the vocabulary lists is one phrase. When systems are
    counted, empty phrases are kept: ``"boiler and radiators, mains gas, ,
    underfloor, electric"`` lists a second system whose name is missing.
    """
    listed = text in vocabulary.lookup or text in vocabulary.discard
    if not vocabulary.split_phrases or listed:
        return [text]
    return text.split(", ") if vocabulary.count_field else phrases(text)


def _lookup(phrase: str, vocabulary: Vocabulary, raw: str) -> Mapping[str, Any]:
    """Return the field values a phrase gives, trying its number as ``#``."""
    if phrase in vocabulary.lookup:
        return vocabulary.lookup[phrase]
    numbers = _NUMBER.findall(phrase)
    template = _NUMBER.sub(NUMBER_PLACEHOLDER, phrase)
    if len(numbers) == 1 and template in vocabulary.lookup:
        return {
            name: (
                parse_number(numbers[0], vocabulary.field_types[name])
                if value == NUMBER_PLACEHOLDER
                else value
            )
            for name, value in vocabulary.lookup[template].items()
        }
    raise UnknownPhraseError(f"{vocabulary.element}: {phrase!r} (in {raw!r})")


def _give(
    fields: dict[str, Any], name: str, value: Any, vocabulary: Vocabulary, raw: str
) -> None:
    """Set a field; a second, different value goes to its second field."""
    if fields.get(name, value) != value:
        second = vocabulary.second_value_fields.get(name)
        if second is None:
            raise PhraseConflictError(
                f"{name} is both {fields[name]!r} and {value!r} in {raw!r}"
            )
        name = second
        if fields.get(name, value) != value:
            raise PhraseConflictError(f"more than two values for {name} in {raw!r}")
    fields[name] = value
