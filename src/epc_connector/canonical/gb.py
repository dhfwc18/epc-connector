"""
Build the Great Britain file of canonical domestic records.

For each origin, the raw file's descriptions are cleaned
(``cleaning.clean_domestic_epc``) and every row is mapped to the canonical
record (``schemas/canonical/gb-domestic.schema.json``) with the origin's
mapping file, ``<origin>-domestic``. The origins are stacked, England and
Wales first, and written in one streaming pass to
``domestic_epc_certificate_gb.parquet`` in the data directory.

``build_gb`` builds the file from every raw domestic file present.
``replace_origin_in_gb`` rebuilds one origin's rows after that origin was
downloaded or updated, copying the other origin's rows from the existing file
instead of cleaning them again.

Every certificate is kept: the canonical record is one certificate, and
``superseded`` marks the ones a newer certificate replaces (known for
Scotland only). Keeping only the latest certificate per UPRN is a query on
this file. Only domestic certificates have a GB file so far.
"""

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import polars as pl

from epc_connector.canonical.mapping import (
    apply_mapping,
    canonical_dtypes,
    check_values,
    load_mapping,
)
from epc_connector.cleaning import clean_domestic_epc
from epc_connector.datasets import Dataset, Origin, Segment, get_dataset
from epc_connector.errors import EpcConnectorError
from epc_connector.io import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    atomic_write,
    drop_page_cache,
    staging_directory,
    stream_to_parquet,
)
from epc_connector.workspace import resolve_data_dir, resolve_staging_dir

logger = logging.getLogger(__name__)

GB_PARQUET_NAME: Final = "domestic_epc_certificate_gb.parquet"
"""File name of the GB domestic file."""
GB_SEGMENTS: Final = frozenset({Segment.DOMESTIC})
"""Segments that have a GB file; the others are not set up yet."""


class GbNotSetUpError(EpcConnectorError, NotImplementedError):
    """A GB file was asked for a segment that does not have one yet."""


def canonical_records(
    origin: Origin | str, *, data_dir: Path | str | None = None, strict: bool = True
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
    data_dir = resolve_data_dir(data_dir)
    mapping = load_mapping(f"{dataset.origin}-{dataset.segment}")
    check_values(pl.scan_parquet(dataset.path(data_dir), low_memory=True), mapping)
    cleaned = clean_domestic_epc(dataset.origin, data_dir=data_dir, strict=strict)
    return apply_mapping(cleaned, mapping, list(canonical_dtypes()))


def gb_origins(data_dir: Path | str | None = None) -> dict[Origin, str]:
    """
    Return the origins in the GB file, with the version each was built from.

    The version is the release or last update of the origin's raw file, as
    stored in the GB file's metadata; only the metadata is read. Empty if
    there is no GB file.
    """
    gb_path = resolve_data_dir(data_dir) / GB_PARQUET_NAME
    if not gb_path.is_file():
        return {}
    stored = pl.read_parquet_metadata(gb_path)
    return {
        dataset.origin: stored[dataset.provenance_key]
        for dataset in _domestic_datasets()
        if dataset.provenance_key in stored
    }


def require_gb_segment(segment: Segment | str) -> None:
    """
    Raise unless ``segment`` has a GB file.

    Raises
    ------
    GbNotSetUpError
        For non-domestic and display certificates.
    """
    if Segment(segment) not in GB_SEGMENTS:
        raise GbNotSetUpError(f"the GB {segment} file is not set up yet")


def build_gb(
    *,
    data_dir: Path | str | None = None,
    staging_dir: Path | str | None = None,
    strict: bool = True,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
) -> Path:
    """
    Write the GB domestic file from every raw domestic file present.

    An origin whose raw file is missing is left out (logged at ``INFO``).
    Each raw file's release or last-update metadata is copied into the GB
    file.

    Parameters
    ----------
    data_dir
        Directory holding the raw files; the GB file is written there. See
        ``workspace.resolve_data_dir``.
    staging_dir
        Parent of the temporary directory used while writing (default
        ``<data_dir>/.staging``).
    strict
        Raise if a description is not covered by the vocabularies; if false,
        mark it ``"not in vocabulary"`` and log a warning instead.
    chunk_rows, row_group_rows
        Memory limits; see ``io.stream_to_parquet``.

    Returns
    -------
    Path
        Path of the GB file.

    Raises
    ------
    FileNotFoundError
        If no raw domestic file is present.
    """
    data_dir = resolve_data_dir(data_dir)
    present = [
        dataset for dataset in _domestic_datasets() if _has_file(dataset, data_dir)
    ]
    if not present:
        raise FileNotFoundError(f"no raw domestic files in {data_dir}")
    for dataset in _domestic_datasets():
        if dataset not in present:
            logger.info("%s is not downloaded; left out of the GB file", dataset.name)
    return _write_gb(
        {dataset.origin: None for dataset in present},
        data_dir=data_dir,
        staging_dir=resolve_staging_dir(staging_dir, data_dir),
        strict=strict,
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
    )


def replace_origin_in_gb(
    origin: Origin | str,
    *,
    data_dir: Path | str | None = None,
    staging_dir: Path | str | None = None,
    strict: bool = True,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
) -> Path:
    """
    Rebuild one origin's rows of the GB file from its raw domestic file.

    The other origin's rows and provenance are copied from the existing GB
    file, so only this origin is cleaned again. Without a GB file, it is
    built from every raw domestic file present (``build_gb``).

    Parameters
    ----------
    origin
        Origin whose raw domestic file changed.
    data_dir, staging_dir, strict, chunk_rows, row_group_rows
        As for ``build_gb``.

    Returns
    -------
    Path
        Path of the GB file.
    """
    data_dir = resolve_data_dir(data_dir)
    gb_path = data_dir / GB_PARQUET_NAME
    if not gb_path.is_file():
        return build_gb(
            data_dir=data_dir,
            staging_dir=staging_dir,
            strict=strict,
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
        )
    changed = Origin(origin)
    logger.debug("Keeping the other origins' rows of %s", gb_path)
    kept = pl.scan_parquet(gb_path, low_memory=True)
    sources = {
        dataset.origin: (
            None
            if dataset.origin == changed
            else kept.filter(pl.col("source_dataset") == dataset.name)
        )
        for dataset in _domestic_datasets()
    }
    return _write_gb(
        sources,
        data_dir=data_dir,
        staging_dir=resolve_staging_dir(staging_dir, data_dir),
        strict=strict,
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
        stored_metadata=pl.read_parquet_metadata(gb_path),
    )


def _write_gb(
    sources: Mapping[Origin, pl.LazyFrame | None],
    *,
    data_dir: Path,
    staging_dir: Path,
    strict: bool,
    chunk_rows: int,
    row_group_rows: int,
    stored_metadata: Mapping[str, str] | None = None,
) -> Path:
    """
    Write the GB file from one row source per origin, in ``Origin`` order.

    ``None`` means "clean the origin's raw file now" (its provenance comes
    from the raw file); a frame is copied as is (its provenance comes from
    ``stored_metadata``, the existing GB file's metadata).
    """
    gb_path = data_dir / GB_PARQUET_NAME
    frames: list[pl.LazyFrame] = []
    metadata: dict[str, str] = {}
    cleaned: list[Dataset] = []
    for origin in Origin:
        if origin not in sources:
            continue
        dataset = get_dataset(origin, Segment.DOMESTIC)
        frame = sources[origin]
        if frame is None:
            logger.debug("Cleaning and mapping %s", dataset.name)
            frames.append(canonical_records(origin, data_dir=data_dir, strict=strict))
            provenance = pl.read_parquet_metadata(dataset.path(data_dir))
            cleaned.append(dataset)
        else:
            frames.append(frame)
            provenance = stored_metadata or {}
        if dataset.provenance_key in provenance:
            metadata[dataset.provenance_key] = provenance[dataset.provenance_key]
    with (
        staging_directory(staging_dir, prefix="gb-domestic-") as run_dir,
        atomic_write(gb_path, run_dir) as written,
    ):
        stream_to_parquet(
            pl.concat(frames),
            written,
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
            metadata=metadata,
        )
    for dataset in cleaned:  # read in full; nothing else needs them cached
        drop_page_cache(dataset.path(data_dir))
    logger.info("Wrote %s", gb_path)
    return gb_path


def _domestic_datasets() -> list[Dataset]:
    return [get_dataset(origin, Segment.DOMESTIC) for origin in Origin]


def _has_file(dataset: Dataset, data_dir: Path) -> bool:
    return dataset.path(data_dir).is_file()
