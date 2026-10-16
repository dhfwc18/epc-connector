"""
Split and normalise raw description text, before any vocabulary is used.
"""

import html
import re
from enum import StrEnum
from typing import Final


class Nation(StrEnum):
    """
    Source nation, which decides what ``|`` means in a description.

    Attributes
    ----------
    ENGLAND_AND_WALES
        ``|`` separates English from Welsh text.
    SCOTLAND
        ``|`` separates building parts (main building, then extensions).
    """

    ENGLAND_AND_WALES = "england-and-wales"
    SCOTLAND = "scotland"


# UTF-8 text that was decoded as Windows-1252 somewhere upstream.
_MISDECODED: Final = {
    "\N{LATIN SMALL LETTER A WITH CIRCUMFLEX}\N{EURO SIGN}\N{TRADE MARK SIGN}": (
        "\N{RIGHT SINGLE QUOTATION MARK}"
    ),
    "\N{LATIN SMALL LETTER A WITH TILDE}\N{CENT SIGN}": (
        "\N{LATIN SMALL LETTER A WITH CIRCUMFLEX}"
    ),
}
_CONTROL_CODE: Final = re.compile(r"^2\d{3} (?=\D)")
_SEPARATOR_RUN: Final = re.compile(r"\?{3,}")
_APOSTROPHE: Final = re.compile(
    r"(?<=\w)(?:\N{RIGHT SINGLE QUOTATION MARK}|\N{ACUTE ACCENT}"
    r"|\N{LATIN SMALL LETTER A WITH CIRCUMFLEX}\?\?|\?|')(?=\w)"
)
_LABEL: Final = re.compile(r"^description:\s*")
_MILLIMETRES: Final = re.compile(r"(\d\+?)\s*mm\b")
_ASSUMED: Final = re.compile(r"(?<=\S)\(assumed\)")
_COMMA: Final = re.compile(r"\s*,\s*")
_SPACES: Final = re.compile(r"\s+")


def split_elements(raw: str, nation: Nation | str) -> list[str]:
    """
    Return the elements of a raw description, before normalisation.

    England and Wales: the English text of a bilingual value. Two parts give
    the first; more parts alternate English and Welsh phrase by phrase, so the
    English is every other part joined. Scotland: one element per building
    part, in order (an empty part stays as ``""``).
    """
    if Nation(nation) is Nation.SCOTLAND:
        return [part.strip() for part in raw.strip().split("|")]
    parts = raw.split("|")
    if len(parts) == 1:
        return [raw.strip()]
    if len(parts) % 2 == 0:
        return ["".join(parts[0::2]).strip()]
    return [parts[0].strip()]


def normalise(element: str) -> str:
    """Return an element in the normalised form used by the vocabularies."""
    text = html.unescape(element)  # "&#0178;" -> superscript two, "&amp;" -> "&"
    text = text.lower()
    for wrong, right in _MISDECODED.items():
        text = text.replace(wrong, right)
    text = _CONTROL_CODE.sub("", text)  # "2207 time and ..." -> "time and ..."
    text = _SEPARATOR_RUN.sub(", ", text)  # "cavity wall???...?as built"
    text = _APOSTROPHE.sub("'", text)  # Welsh "y'u" written with ', ?, ...
    text = text.replace("&", " and ")
    text = _LABEL.sub("", text)  # "description: single glazed"
    text = _MILLIMETRES.sub(r"\1 mm", text)  # "300+mm" -> "300+ mm"
    text = _ASSUMED.sub(" (assumed)", text)  # "insulation(assumed)"
    text = _COMMA.sub(", ", text)
    return _SPACES.sub(" ", text).strip(" ,")


def phrases(element: str) -> list[str]:
    """Return the non-empty comma-separated phrases of a normalised element."""
    return [phrase for phrase in element.split(", ") if phrase]
