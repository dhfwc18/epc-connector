"""
Turn a setup or update request into the ordered steps that carry it out.

A plan downloads (``setup``) or updates (``update``) each selected dataset,
smallest first, then brings the GB file up to date: one ``build_gb`` when
every origin's domestic data changed, otherwise ``replace_origin_in_gb`` for
the origin that did. Segments without a GB file are listed as not set up.
Plans are plain data, so they can be checked before anything runs.
"""

import datetime as dt
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from epc_connector import sources
from epc_connector.canonical import gb
from epc_connector.datasets import Dataset, Origin, Segment, select_datasets
from epc_connector.storage import DEFAULT_CHUNK_ROWS, DEFAULT_ROW_GROUP_ROWS


@dataclass(frozen=True)
class Workspace:
    """
    Where the pipeline reads and writes, and its memory limits.

    Attributes
    ----------
    data_dir
        Directory of the raw and GB Parquet files.
    staging_dir
        Parent of the temporary directories used while writing.
    chunk_rows, row_group_rows
        Memory limits; see ``storage.stream_to_parquet``.
    """

    data_dir: Path = Path("data")
    staging_dir: Path = Path(".staging")
    chunk_rows: int = DEFAULT_CHUNK_ROWS
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS

    def options(self) -> dict[str, Any]:
        """Keyword arguments every download, update and GB function takes."""
        return {
            "data_dir": self.data_dir,
            "staging_dir": self.staging_dir,
            "chunk_rows": self.chunk_rows,
            "row_group_rows": self.row_group_rows,
        }


DEFAULT_WORKSPACE = Workspace()
"""``data`` and ``.staging`` under the working directory, default limits."""


@dataclass(frozen=True)
class Step:
    """
    One unit of work, run in its own process.

    Attributes
    ----------
    name
        What the step does, e.g. ``"download scotland/domestic"``.
    function
        Module-level function to call.
    args, kwargs
        Arguments for ``function``.
    """

    name: str
    function: Callable[..., Path]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Plan:
    """
    The steps of a setup or update, and what it leaves out.

    Attributes
    ----------
    datasets
        Datasets downloaded or updated, in order.
    steps
        Steps to run, in order.
    not_set_up
        Selected segments that have no GB file yet.
    """

    datasets: list[Dataset]
    steps: list[Step]
    not_set_up: list[Segment]


def plan_setup(
    origins: Iterable[Origin],
    segments: Iterable[Segment] | None = None,
    *,
    workspace: Workspace = DEFAULT_WORKSPACE,
    strict: bool = True,
) -> Plan:
    """
    Plan downloading datasets and bringing the GB file up to date.

    Parameters
    ----------
    origins, segments
        Datasets to download; see ``datasets.select_datasets``.
    workspace
        Directories and memory limits.
    strict
        Whether descriptions the vocabularies do not cover stop the GB step.

    Raises
    ------
    DatasetError
        If no published dataset matches.
    """
    datasets = select_datasets(origins, segments)
    steps = [
        Step(
            f"download {dataset.name}",
            sources.download,
            (dataset.origin, dataset.segment),
            workspace.options(),
        )
        for dataset in datasets
    ]
    return _with_gb_steps(datasets, steps, workspace, strict)


def plan_update(
    origins: Iterable[Origin],
    segments: Iterable[Segment] | None = None,
    *,
    date_from: dt.date | str | None = None,
    date_to: dt.date | str | None = None,
    workspace: Workspace = DEFAULT_WORKSPACE,
    strict: bool = True,
) -> Plan:
    """
    Plan updating downloaded datasets and the GB file.

    Parameters
    ----------
    origins, segments
        Datasets to update; see ``datasets.select_datasets``.
    date_from, date_to
        Window to refresh: lodgement date for England and Wales,
        ``CREATED_AT`` date for Scotland; see each source's ``update``.
    workspace, strict
        As for ``plan_setup``.

    Raises
    ------
    DatasetError
        If no published dataset matches.
    """
    datasets = select_datasets(origins, segments)
    window = {"date_from": date_from, "date_to": date_to}
    steps = [
        Step(
            f"update {dataset.name}",
            sources.update,
            (dataset.origin, dataset.segment),
            window | workspace.options(),
        )
        for dataset in datasets
    ]
    return _with_gb_steps(datasets, steps, workspace, strict)


def _with_gb_steps(
    datasets: list[Dataset], steps: list[Step], workspace: Workspace, strict: bool
) -> Plan:
    """Append the step that brings the GB file up to date, if any."""
    changed = [d.origin for d in datasets if d.segment in gb.GB_SEGMENTS]
    not_set_up = [s for s in Segment if s not in gb.GB_SEGMENTS]
    selected = {dataset.segment for dataset in datasets}
    options = workspace.options() | {"strict": strict}
    if set(changed) == set(Origin):
        steps = [*steps, Step("build GB domestic", gb.build_gb, (), options)]
    elif changed:
        [origin] = changed
        name = f"replace {origin} in GB domestic"
        steps = [*steps, Step(name, gb.replace_origin_in_gb, (origin,), options)]
    return Plan(
        datasets=datasets,
        steps=steps,
        not_set_up=[segment for segment in not_set_up if segment in selected],
    )
