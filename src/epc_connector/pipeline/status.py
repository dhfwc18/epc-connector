"""
Report which datasets and GB files are set up in a data directory.

Only file metadata is read, so a report takes milliseconds.
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from epc_connector.canonical import GB_PARQUET_NAME, GB_SEGMENTS, gb_origins
from epc_connector.datasets import DATASETS, Segment

SET_UP = "[v]"
"""Mark for a file that exists."""
MISSING = "[ ]"
"""Mark for a file that does not exist yet."""
NAME_WIDTH = max(len(dataset.name) for dataset in DATASETS.values())
"""Width of the name column, so the file names line up."""


@dataclass(frozen=True)
class DatasetStatus:
    """
    Whether one dataset or GB file is set up.

    Attributes
    ----------
    name
        ``"<origin>/<segment>"`` for a dataset, ``"gb/<segment>"`` for a GB
        file.
    path
        The file, or ``None`` for a segment that has no GB file yet.
    set_up
        Whether the file exists.
    version
        Release or last update from the file's metadata; for a GB file,
        that of each origin it holds, e.g. ``"scotland 2026q2"``.
    """

    name: str
    path: Path | None
    set_up: bool
    version: str | None = None


def dataset_statuses(data_dir: Path) -> list[DatasetStatus]:
    """Return the status of every dataset, then of every GB file."""
    return _raw_statuses(data_dir) + _gb_statuses(data_dir)


def status_lines(data_dir: Path) -> list[str]:
    """
    Return a report line per dataset and per GB file, marking what exists.

    A present file shows its version; GB files for segments without one yet
    say so.
    """
    return [
        f"Datasets in {data_dir}/:",
        *map(_line, _raw_statuses(data_dir)),
        "GB files:",
        *map(_line, _gb_statuses(data_dir)),
    ]


def _raw_statuses(data_dir: Path) -> list[DatasetStatus]:
    statuses = []
    for dataset in DATASETS.values():
        path = dataset.path(data_dir)
        version = None
        if path.is_file():
            version = pl.read_parquet_metadata(path).get(dataset.provenance_key)
        statuses.append(DatasetStatus(dataset.name, path, path.is_file(), version))
    return statuses


def _gb_statuses(data_dir: Path) -> list[DatasetStatus]:
    statuses = []
    for segment in Segment:
        name = f"gb/{segment}"
        if segment not in GB_SEGMENTS:
            statuses.append(DatasetStatus(name, None, set_up=False))
            continue
        path = data_dir / GB_PARQUET_NAME
        origins = gb_origins(data_dir)
        version = ", ".join(f"{o} {v}" for o, v in origins.items()) or None
        statuses.append(DatasetStatus(name, path, path.is_file(), version))
    return statuses


def _line(status: DatasetStatus) -> str:
    mark = SET_UP if status.set_up else MISSING
    if status.path is None:
        detail = "not set up yet"
    else:
        detail = status.path.name
        if status.version:
            detail += f" ({status.version})"
    return f"  {mark} {status.name:<{NAME_WIDTH}} {detail}"
