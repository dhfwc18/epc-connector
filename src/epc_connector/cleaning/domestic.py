"""
Clean the descriptions of a raw domestic EPC file, lazily.
"""

from pathlib import Path
from typing import Final

import polars as pl

from epc_connector.cleaning.descriptions import clean_values
from epc_connector.cleaning.vocabulary import DEFAULT_VOCABULARIES_DIR, Element
from epc_connector.datasets import Origin, Segment, get_dataset
from epc_connector.workspace import resolve_data_dir

SOURCE_COLUMNS: Final[dict[Origin, dict[Element, str]]] = {
    Origin.ENGLAND_AND_WALES: {
        Element.WALLS: "walls_description",
        Element.ROOF: "roof_description",
        Element.FLOOR: "floor_description",
        Element.WINDOWS: "windows_description",
        Element.GLAZED_TYPE: "glazed_type",
        Element.LIGHTING: "lighting_description",
        Element.MAIN_HEATING: "mainheat_description",
        Element.HEATING_CONTROLS: "mainheatcont_description",
        Element.HOT_WATER: "hotwater_description",
        Element.SECONDARY_HEATING: "secondheat_description",
        Element.MAIN_FUEL: "main_fuel",
    },
    Origin.SCOTLAND: {
        Element.WALLS: "WALL_DESCRIPTION",
        Element.ROOF: "ROOF_DESCRIPTION",
        Element.FLOOR: "FLOOR_DESCRIPTION",
        Element.WINDOWS: "WINDOWS_DESCRIPTION",
        Element.GLAZED_TYPE: "GLAZED_TYPE",
        Element.LIGHTING: "LIGHTING_DESCRIPTION",
        Element.MAIN_HEATING: "MAINHEAT_DESCRIPTION",
        Element.HEATING_CONTROLS: "MAINHEATCONT_DESCRIPTION",
        Element.HOT_WATER: "HOTWATER_DESCRIPTION",
        Element.SECONDARY_HEATING: "SECONDHEAT_DESCRIPTION",
        Element.MAIN_FUEL: "MAIN_FUEL",
    },
}
"""Domestic source column holding each element's description, by origin."""


def clean_domestic_epc(
    origin: Origin | str,
    *,
    data_dir: Path | str | None = None,
    strict: bool = True,
    vocabularies_dir: Path = DEFAULT_VOCABULARIES_DIR,
) -> pl.LazyFrame:
    """
    Return a raw domestic file with its descriptions cleaned, as a lazy frame.

    Every column of the origin's raw domestic file (``datasets.DATASETS``) is
    kept, in order, except that each description column is replaced by its
    element's output columns (``Vocabulary.columns``). The distinct
    descriptions are read in one streaming pass and cleaned once; the frame
    joins them back, so nothing else is read until it is collected or sunk.

    Parameters
    ----------
    origin
        ``"england-and-wales"`` or ``"scotland"``.
    data_dir
        Directory holding the raw file; see ``workspace.resolve_data_dir``.
    strict
        Raise if a description is not covered by the vocabularies; if false,
        mark it ``"not in vocabulary"`` and log a warning instead.
    vocabularies_dir
        Directory holding the vocabulary files.

    Raises
    ------
    UnknownPhraseError
        In strict mode, if the vocabularies do not cover a description.
    """
    dataset = get_dataset(origin, Segment.DOMESTIC)
    columns = SOURCE_COLUMNS[dataset.origin]
    elements = {column: element for element, column in columns.items()}
    source = pl.scan_parquet(dataset.path(resolve_data_dir(data_dir)), low_memory=True)
    distinct = source.select(
        pl.col(column).unique().implode() for column in elements
    ).collect(engine="streaming")

    output: list[str] = []
    for column in source.collect_schema().names():
        if column not in elements:
            output.append(column)
            continue
        table = clean_values(
            distinct[column][0],
            elements[column],
            dataset.origin,
            strict=strict,
            vocabularies_dir=vocabularies_dir,
        ).rename({"raw": column})
        source = source.join(table.lazy(), on=column, how="left", maintain_order="left")
        output.extend(table.columns[1:])
    return source.select(output)
