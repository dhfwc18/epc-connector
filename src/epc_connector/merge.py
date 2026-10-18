"""
Merge the domestic EPCs of England and Wales and of Scotland into one GB file.

For each origin, the raw file's descriptions are cleaned
(``cleaning.clean_domestic_epc``) and every row is mapped to the canonical
record (``schemas/canonical/gb-domestic.schema.json``) with that origin's
mapping file. The two are stacked and written in one streaming pass to
``data/domestic_epc_certificate_gb.parquet``.

Every certificate is kept: the canonical record is one certificate, and
``superseded`` marks the ones a newer certificate replaces (known for Scotland
only). Keeping only the latest certificate per UPRN is a query on this file.
"""

import logging
from pathlib import Path
from typing import Final

import polars as pl

from epc_connector.cleaning import clean_domestic_epc
from epc_connector.datasets import Origin, Segment, get_dataset
from epc_connector.mapping import (
    apply_mapping,
    canonical_dtypes,
    check_values,
    load_mapping,
)
from epc_connector.storage import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    atomic_write,
    drop_page_cache,
    staging_directory,
    stream_to_parquet,
)

logger = logging.getLogger(__name__)

GB_PARQUET_NAME: Final = "domestic_epc_certificate_gb.parquet"
"""File name of the merged GB domestic file."""
MAPPING_NAMES: Final[dict[Origin, str]] = {
    Origin.ENGLAND_AND_WALES: "england-and-wales-domestic",
    Origin.SCOTLAND: "scotland-domestic",
}
"""Mapping file name (without suffix), by origin."""


def canonical_domestic_epc(
    origin: Origin | str, *, data_dir: Path | str = "data", strict: bool = True
) -> pl.LazyFrame:
    """
    Return one origin's domestic certificates as canonical records, lazily.

    The mapping's value checks run first on the raw file's distinct values,
    then the descriptions are cleaned; the returned frame does the rest when
    it is collected or sunk.

    Raises
    ------
    MappingError
        If a raw value is not handled by the origin's mapping.
    UnknownPhraseError
        In strict mode, if the vocabularies do not cover a description.
    """
    dataset = get_dataset(origin, Segment.DOMESTIC)
    mapping = load_mapping(MAPPING_NAMES[dataset.origin])
    raw = pl.scan_parquet(dataset.path(data_dir), low_memory=True)
    check_values(raw, mapping)
    cleaned = clean_domestic_epc(dataset.origin, data_dir=data_dir, strict=strict)
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
        Memory limits; see ``storage.stream_to_parquet``.

    Returns
    -------
    Path
        Path of the GB file.
    """
    data_dir = Path(data_dir)
    output_path = data_dir / GB_PARQUET_NAME
    frames = [
        canonical_domestic_epc(origin, data_dir=data_dir, strict=strict)
        for origin in MAPPING_NAMES
    ]
    raw_datasets = [get_dataset(origin, Segment.DOMESTIC) for origin in MAPPING_NAMES]
    metadata = {}
    for dataset in raw_datasets:  # each raw file's release or last update
        stored = pl.read_parquet_metadata(dataset.path(data_dir))
        if dataset.provenance_key in stored:
            metadata[dataset.provenance_key] = stored[dataset.provenance_key]
    with (
        staging_directory(Path(staging_dir), prefix="gb-domestic-") as run_dir,
        atomic_write(output_path, run_dir) as written,
    ):
        stream_to_parquet(
            pl.concat(frames),
            written,
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
            metadata=metadata,
        )
    for dataset in raw_datasets:  # read in full; nothing else needs them cached
        drop_page_cache(dataset.path(data_dir))
    logger.info("Wrote %s", output_path)
    return output_path
