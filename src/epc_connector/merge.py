"""
Merge the domestic EPCs of England and Wales and of Scotland into one GB file.

For each nation, the raw file's descriptions are cleaned
(``cleaning.clean_domestic_epc``) and every row is mapped to the canonical
record (``schemas/canonical/gb-domestic.schema.json``) with that nation's
mapping file. The two are stacked and written in one streaming pass to
``data/domestic_epc_certificate_gb.parquet``.

Every certificate is kept: the canonical record is one certificate, and
``superseded`` marks the ones a newer certificate replaces (known for Scotland
only). Keeping only the latest certificate per UPRN is a query on this file.
"""

import logging
import shutil
from pathlib import Path
from typing import Final

import polars as pl

from epc_connector.cleaning import RAW_PARQUET_NAMES, Nation, clean_domestic_epc
from epc_connector.external.england_wales_epc import LAST_UPDATED_METADATA_KEY
from epc_connector.external.scotland_epc import RELEASE_METADATA_KEY
from epc_connector.ingest import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    staging_directory,
    stream_to_parquet,
)
from epc_connector.mapping import (
    apply_mapping,
    canonical_dtypes,
    check_values,
    load_mapping,
)

logger = logging.getLogger(__name__)

GB_PARQUET_NAME: Final = "domestic_epc_certificate_gb.parquet"
MAPPING_NAMES: Final[dict[Nation, str]] = {
    Nation.ENGLAND_AND_WALES: "england-and-wales-domestic",
    Nation.SCOTLAND: "scotland-domestic",
}
# Provenance copied from each raw file into the GB file's metadata.
PROVENANCE_KEYS: Final[dict[Nation, str]] = {
    Nation.ENGLAND_AND_WALES: LAST_UPDATED_METADATA_KEY,
    Nation.SCOTLAND: RELEASE_METADATA_KEY,
}


def canonical_domestic_epc(
    nation: Nation | str, *, data_dir: Path | str = "data", strict: bool = True
) -> pl.LazyFrame:
    """
    Return one nation's domestic certificates as canonical records, lazily.

    The mapping's value checks run first on the raw file's distinct values,
    then the descriptions are cleaned; the returned frame does the rest when
    it is collected or sunk.

    Raises
    ------
    MappingError
        If a raw value is not handled by the nation's mapping.
    UnknownPhraseError
        In strict mode, if the vocabularies do not cover a description.
    """
    nation = Nation(nation)
    mapping = load_mapping(MAPPING_NAMES[nation])
    raw_path = Path(data_dir) / RAW_PARQUET_NAMES[nation]
    check_values(pl.scan_parquet(raw_path, low_memory=True), mapping)
    cleaned = clean_domestic_epc(nation, data_dir=data_dir, strict=strict)
    return apply_mapping(cleaned, mapping, list(canonical_dtypes()))


def merge_gb_domestic_epc(
    *,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    strict: bool = True,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
) -> Path:
    """
    Write ``data/domestic_epc_certificate_gb.parquet`` from both raw files.

    Rows are England and Wales then Scotland, each in raw file order. The
    raw files' release and last-update metadata are copied.

    Parameters
    ----------
    data_dir
        Directory holding the raw files; the GB file is written there.
    staging_dir
        Parent of the temporary directory used while writing.
    strict
        Raise if a description is not covered by the vocabularies; if false,
        mark it ``"not in vocabulary"`` and log a warning instead.
    chunk_rows, row_group_rows
        Memory limits; see ``ingest.stream_to_parquet``.

    Returns
    -------
    Path
        Path of the GB file.
    """
    data_dir = Path(data_dir)
    output_path = data_dir / GB_PARQUET_NAME
    frames = [
        canonical_domestic_epc(nation, data_dir=data_dir, strict=strict)
        for nation in MAPPING_NAMES
    ]
    metadata = {}
    for nation, key in PROVENANCE_KEYS.items():
        stored = pl.read_parquet_metadata(data_dir / RAW_PARQUET_NAMES[nation])
        if key in stored:
            metadata[key] = stored[key]
    with staging_directory(Path(staging_dir), prefix="gb-domestic-") as run_dir:
        written = run_dir / GB_PARQUET_NAME
        stream_to_parquet(
            pl.concat(frames),
            written,
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
            metadata=metadata,
        )
        shutil.move(written, output_path)
    logger.info("Wrote %s", output_path)
    return output_path
