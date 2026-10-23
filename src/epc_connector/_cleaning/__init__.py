"""
Clean the free-text building descriptions of domestic EPCs into typed columns.

Internal: not part of the public API (see ``epc_connector``).

Each described element (walls, roof, main heating, ...) has a vocabulary file,
``schemas/vocabularies/<element>.vocabulary.json``, that lists for every
output field which phrases give which value. A description is cleaned in four
steps:

1. ``text.split_elements``: the English part of an England and Wales
   ``english|welsh`` value, or the building parts of a Scottish
   ``main | extension`` value.
2. ``text.normalise``: case, encoding faults and spacing.
3. Split the element on commas into phrases, unless the whole element is
   listed in the vocabulary.
4. ``descriptions.clean_description``: look each phrase up exactly. A phrase
   that is not listed is tried again with its number replaced by ``#``
   (``"250 mm loft insulation"`` -> ``"# mm loft insulation"``); the number
   then fills the field that lists that phrase.

A phrase that is not listed, or two phrases that give one field different
values, raise an error instead of being guessed. Raw values repeat heavily
(about 22,000 distinct descriptions across 34 million certificates), so
``domestic.clean_domestic_epc`` cleans each distinct value once and returns a
lazy frame that joins the result back onto the rows.

Modules
-------
text
    Splitting and normalising raw text; no vocabulary involved.
vocabulary
    Reading a vocabulary file into a phrase lookup.
descriptions
    Cleaning descriptions with a vocabulary.
domestic
    Cleaning a raw domestic file, as a lazy frame.
"""

from epc_connector._cleaning.descriptions import (
    CleanedDescription,
    PhraseConflictError,
    UnknownPhraseError,
    clean_description,
    clean_values,
)
from epc_connector._cleaning.domestic import (
    SOURCE_COLUMNS,
    clean_domestic_epc,
)
from epc_connector._cleaning.vocabulary import Element, Vocabulary, load_vocabulary

__all__ = [
    "SOURCE_COLUMNS",
    "CleanedDescription",
    "Element",
    "PhraseConflictError",
    "UnknownPhraseError",
    "Vocabulary",
    "clean_description",
    "clean_domestic_epc",
    "clean_values",
    "load_vocabulary",
]
