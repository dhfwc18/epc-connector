"""
Report which datasets and GB files are set up in a data directory.
"""

from pathlib import Path

import polars as pl

from epc_connector.canonical import GB_PARQUET_NAME, GB_SEGMENTS
from epc_connector.datasets import DATASETS, Dataset, Segment

SET_UP = "[v]"
"""Mark for a file that exists."""
MISSING = "[ ]"
"""Mark for a file that does not exist yet."""
NAME_WIDTH = max(len(dataset.name) for dataset in DATASETS.values())
"""Width of the name column, so the file names line up."""


def status_lines(data_dir: Path | str) -> list[str]:
    """
    Return a report line per dataset and per GB file, marking what exists.

    A present file shows its release or last update (from its metadata).
    GB files for segments without one yet say so.
    """
    data_dir = Path(data_dir)
    lines = [f"Datasets in {data_dir}/:"]
    lines += [_dataset_line(dataset, data_dir) for dataset in DATASETS.values()]
    lines.append("GB files:")
    for segment in Segment:
        name = f"gb/{segment}"
        if segment not in GB_SEGMENTS:
            lines.append(f"  {MISSING} {name:<{NAME_WIDTH}} not set up yet")
            continue
        path = data_dir / GB_PARQUET_NAME
        mark = SET_UP if path.is_file() else MISSING
        lines.append(f"  {mark} {name:<{NAME_WIDTH}} {path.name}")
    return lines


def _dataset_line(dataset: Dataset, data_dir: Path) -> str:
    path = dataset.path(data_dir)
    if not path.is_file():
        return f"  {MISSING} {dataset.name:<{NAME_WIDTH}} {path.name}"
    stamp = pl.read_parquet_metadata(path).get(dataset.provenance_key)
    detail = f" ({stamp})" if stamp else ""
    return f"  {SET_UP} {dataset.name:<{NAME_WIDTH}} {path.name}{detail}"
