"""
Turn a setup or update request into the ordered steps that carry it out.

A plan downloads (``setup``) or updates (``update``) each selected dataset,
smallest first, then brings the GB file up to date: one ``build_gb`` when
every origin's domestic data changed, otherwise ``replace_origin_in_gb`` for
the origin that did. ``setup`` skips datasets already in the data directory
unless forced, so running it again only fills the gaps, and ``update`` skips
datasets never downloaded. ``plan_domestic`` sets up only what loading one
origin's domestic data needs. Segments without a GB file are listed as not
set up. Plans are plain data, so they can be
checked before anything runs.
"""

import datetime as dt
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import polars as pl

from epc_connector import sources
from epc_connector.canonical import gb
from epc_connector.datasets import (
    Dataset,
    Origin,
    Segment,
    get_dataset,
    select_datasets,
)
from epc_connector.workspace import Workspace


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
        Arguments for ``function``; left out of the step's ``repr`` so an
        API token never reaches a log or traceback.
    """

    name: str
    function: Callable[..., Path]
    args: tuple[Any, ...] = field(default=(), repr=False)
    kwargs: Mapping[str, Any] = field(default_factory=dict, repr=False)


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
        Segments of ``datasets`` that have no GB file yet.
    skipped
        Selected datasets already downloaded, which ``setup`` leaves alone.
    not_downloaded
        Selected datasets never downloaded, which ``update`` leaves alone.
    downloads
        Datasets the plan downloads in full (not updates), in order.
    """

    datasets: list[Dataset]
    steps: list[Step]
    not_set_up: list[Segment]
    skipped: list[Dataset] = field(default_factory=list)
    not_downloaded: list[Dataset] = field(default_factory=list)
    downloads: list[Dataset] = field(default_factory=list)


def plan_setup(
    origins: Iterable[Origin],
    segments: Iterable[Segment] | None = None,
    *,
    workspace: Workspace | None = None,
    strict: bool = True,
    force: bool = False,
    api_token: str | None = None,
) -> Plan:
    """
    Plan downloading datasets and bringing the GB file up to date.

    Datasets already in the data directory are skipped unless ``force``.
    The GB file is built if a domestic dataset was downloaded, or if it is
    missing although its raw files are there.

    Parameters
    ----------
    origins, segments
        Datasets to download; see ``datasets.select_datasets``.
    workspace
        Directories and memory limits; ``Workspace.resolve()`` if omitted.
    strict
        Whether descriptions the vocabularies do not cover stop the GB step.
    force
        Download every selected dataset, even one already downloaded.
    api_token
        England and Wales API token for its steps; if ``None``, each step
        finds one as ``credentials`` describes.

    Raises
    ------
    DatasetError
        If no published dataset matches.
    """
    workspace = workspace or Workspace.resolve()
    selected = select_datasets(origins, segments)
    skipped = (
        [] if force else [d for d in selected if d.path(workspace.data_dir).is_file()]
    )
    datasets = [dataset for dataset in selected if dataset not in skipped]
    gb_missing = not (workspace.data_dir / gb.GB_PARQUET_NAME).is_file()
    rebuild = gb_missing and any(d.segment in gb.GB_SEGMENTS for d in skipped)
    steps = [
        Step(
            f"download {dataset.name}",
            sources.download,
            (dataset.origin, dataset.segment),
            _source_options(dataset, workspace, api_token),
        )
        for dataset in datasets
    ]
    plan = _with_gb_steps(datasets, steps, workspace, strict, rebuild=rebuild)
    return replace(plan, skipped=skipped, downloads=datasets)


def plan_update(
    origins: Iterable[Origin],
    segments: Iterable[Segment] | None = None,
    *,
    date_from: dt.date | str | None = None,
    date_to: dt.date | str | None = None,
    workspace: Workspace | None = None,
    strict: bool = True,
    api_token: str | None = None,
) -> Plan:
    """
    Plan updating downloaded datasets and the GB file.

    Selected datasets that were never downloaded are left out.

    Parameters
    ----------
    origins, segments
        Datasets to update; see ``datasets.select_datasets``.
    date_from, date_to
        Window to refresh: lodgement date for England and Wales,
        ``CREATED_AT`` date for Scotland; see each source's ``update``.
    workspace, strict, api_token
        As for ``plan_setup``.

    Raises
    ------
    DatasetError
        If no published dataset matches.
    """
    workspace = workspace or Workspace.resolve()
    selected = select_datasets(origins, segments)
    datasets = [d for d in selected if d.path(workspace.data_dir).is_file()]
    not_downloaded = [dataset for dataset in selected if dataset not in datasets]
    window = {"date_from": date_from, "date_to": date_to}
    steps = [
        Step(
            f"update {dataset.name}",
            sources.update,
            (dataset.origin, dataset.segment),
            window | _source_options(dataset, workspace, api_token),
        )
        for dataset in datasets
    ]
    plan = _with_gb_steps(datasets, steps, workspace, strict)
    return replace(plan, not_downloaded=not_downloaded)


def plan_domestic(
    origin: Origin,
    *,
    workspace: Workspace | None = None,
    strict: bool = True,
    api_token: str | None = None,
    force: bool = False,
) -> Plan:
    """
    Plan what loading one origin's domestic certificates needs, and no more.

    The origin's raw domestic file is downloaded if missing (or always, with
    ``force``). Its rows in the
    GB file are rebuilt if they are missing or were built from another
    version of the raw file (compared through the files' metadata). Other
    origins and segments are not downloaded.

    Parameters
    ----------
    origin
        Origin to load.
    workspace, strict, api_token
        As for ``plan_setup``.
    force
        Download the raw file even if it is already there.
    """
    workspace = workspace or Workspace.resolve()
    dataset = get_dataset(origin, Segment.DOMESTIC)
    raw_path = dataset.path(workspace.data_dir)
    datasets = [dataset] if force or not raw_path.is_file() else []
    steps = [
        Step(
            f"download {dataset.name}",
            sources.download,
            (dataset.origin, dataset.segment),
            _source_options(dataset, workspace, api_token),
        )
        for dataset in datasets
    ]
    in_gb = gb.gb_origins(workspace.data_dir).get(dataset.origin)
    current = (
        not datasets
        and in_gb is not None
        and in_gb == pl.read_parquet_metadata(raw_path).get(dataset.provenance_key)
    )
    if not current:
        options = workspace.options() | {"strict": strict}
        name = f"replace {dataset.origin} in GB domestic"
        steps.append(Step(name, gb.replace_origin_in_gb, (dataset.origin,), options))
    return Plan(datasets=datasets, steps=steps, not_set_up=[], downloads=datasets)


def _source_options(
    dataset: Dataset, workspace: Workspace, api_token: str | None
) -> dict[str, Any]:
    """Keyword arguments of a download or update step for ``dataset``."""
    options = workspace.options()
    if api_token is not None and dataset.origin is Origin.ENGLAND_AND_WALES:
        options["api_token"] = api_token
    return options


def _with_gb_steps(
    datasets: list[Dataset],
    steps: list[Step],
    workspace: Workspace,
    strict: bool,
    *,
    rebuild: bool = False,
) -> Plan:
    """
    Append the step that brings the GB file up to date, if any.

    ``rebuild`` builds the GB file even if no domestic dataset changed.
    """
    changed = [d.origin for d in datasets if d.segment in gb.GB_SEGMENTS]
    not_set_up = [s for s in Segment if s not in gb.GB_SEGMENTS]
    selected = {dataset.segment for dataset in datasets}
    options = workspace.options() | {"strict": strict}
    if set(changed) == set(Origin) or (rebuild and not changed):
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
