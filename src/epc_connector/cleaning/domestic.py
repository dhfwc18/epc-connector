"""
Write the cleaned domestic EPC files next to the raw ones.
"""

import logging
import shutil
from pathlib import Path
from typing import Final

import polars as pl

from epc_connector.cleaning.descriptions import clean_values
from epc_connector.cleaning.text import Nation
from epc_connector.cleaning.vocabulary import DEFAULT_VOCABULARIES_DIR, Element
from epc_connector.external import england_wales_epc, scotland_epc
from epc_connector.ingest import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    staging_directory,
    stream_to_parquet,
)

logger = logging.getLogger(__name__)

SOURCE_COLUMNS: Final[dict[Nation, dict[Element, str]]] = {
    Nation.ENGLAND_AND_WALES: {
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
    Nation.SCOTLAND: {
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
"""Domestic source column holding each element's description, by nation."""

RAW_PARQUET_NAMES: Final[dict[Nation, str]] = {
    Nation.ENGLAND_AND_WALES: england_wales_epc.PARQUET_NAMES[
        england_wales_epc.EnglandWalesEpcDataset.DOMESTIC
    ],
    Nation.SCOTLAND: scotland_epc.PARQUET_NAMES[
        scotland_epc.ScotlandEpcDataset.DOMESTIC
    ],
}
CLEANED_PARQUET_NAMES: Final[dict[Nation, str]] = {
    nation: f"cleaned_{name}" for nation, name in RAW_PARQUET_NAMES.items()
}
# Provenance copied from the raw file. Only this key: the raw file's other
# metadata includes "ARROW:schema", which would give readers the raw columns.
_PROVENANCE_KEYS: Final[dict[Nation, str]] = {
    Nation.ENGLAND_AND_WALES: england_wales_epc.LAST_UPDATED_METADATA_KEY,
    Nation.SCOTLAND: scotland_epc.RELEASE_METADATA_KEY,
}


def clean_domestic_epc(
    nation: Nation | str,
    *,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    strict: bool = True,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    vocabularies_dir: Path = DEFAULT_VOCABULARIES_DIR,
) -> Path:
    """
    Write ``data/cleaned_domestic_epc_certificate_<nation>.parquet``.

    The cleaned file has every column of the raw domestic file, in order,
    except that each description column is replaced by its element's output
    columns (``Vocabulary.columns``). The raw file is not changed, and its
    release or last-update metadata is copied. Distinct descriptions are
    cleaned once and joined back, and the result is streamed to disk.

    Parameters
    ----------
    nation
        ``"england-and-wales"`` or ``"scotland"``.
    data_dir
        Directory holding the raw file; the cleaned file is written there.
    staging_dir
        Parent of the temporary directory used while writing.
    strict
        Raise if a description is not covered by the vocabularies; if false,
        mark it ``"not in vocabulary"`` and log a warning instead.
    chunk_rows, row_group_rows
        Memory limits; see ``ingest.stream_to_parquet``.
    vocabularies_dir
        Directory holding the vocabulary files.

    Returns
    -------
    Path
        Path of the cleaned file.
    """
    nation = Nation(nation)
    raw_path = Path(data_dir) / RAW_PARQUET_NAMES[nation]
    cleaned_path = Path(data_dir) / CLEANED_PARQUET_NAMES[nation]
    elements = {column: element for element, column in SOURCE_COLUMNS[nation].items()}
    source = pl.scan_parquet(raw_path, low_memory=True)
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
            nation,
            strict=strict,
            vocabularies_dir=vocabularies_dir,
        ).rename({"raw": column})
        source = source.join(table.lazy(), on=column, how="left", maintain_order="left")
        output.extend(table.columns[1:])

    provenance = _PROVENANCE_KEYS[nation]
    stored = pl.read_parquet_metadata(raw_path)
    metadata = {provenance: stored[provenance]} if provenance in stored else None
    with staging_directory(Path(staging_dir), prefix=f"clean-{nation}-") as run_dir:
        written = run_dir / cleaned_path.name
        stream_to_parquet(
            source.select(output),
            written,
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
            metadata=metadata,
        )
        shutil.move(written, cleaned_path)
    logger.info("Wrote %s", cleaned_path)
    return cleaned_path
