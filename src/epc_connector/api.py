"""
The library: load domestic certificates, update the data, see what is set up.

``load_domestic_certificates`` sets up what it needs on first use: the raw
domestic file of each origin asked for and that origin's rows in the GB file.
Other origins and segments are never downloaded. ``update`` refreshes the
data, exactly as ``epc-connector update`` does. Files live in the data
directory (see ``workspace``).

Steps run in the calling process; the ``epc-connector`` command runs each in
a fresh one.
"""

import calendar
import datetime as dt
import logging
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Final, Literal, overload

import httpx
import polars as pl

from epc_connector.canonical import GB_PARQUET_NAME, gb_origins
from epc_connector.datasets import (
    Dataset,
    Origin,
    Segment,
    get_dataset,
    parse_origins,
)
from epc_connector.environment import report_unknown_variables
from epc_connector.errors import EpcConnectorError
from epc_connector.pipeline.plan import plan_domestic, plan_update
from epc_connector.pipeline.run import run_plan
from epc_connector.pipeline.status import DatasetStatus, dataset_statuses
from epc_connector.sources import england_and_wales, scotland
from epc_connector.workspace import Workspace, lock_data_dir, resolve_data_dir

logger = logging.getLogger(__name__)

STALE_AFTER_MONTHS: Final = 3
"""Age, in months, after which loaded data comes with a warning."""

# Date the data runs to, from the version stored in the GB file's metadata.
_DATA_DATES: Final[dict[Origin, Callable[[str], dt.date]]] = {
    Origin.ENGLAND_AND_WALES: england_and_wales.last_updated_date,
    Origin.SCOTLAND: scotland.release_end_date,
}
# Setup failures that leave GB data with the other origin only.
_SETUP_ERRORS: Final = (EpcConnectorError, httpx.HTTPError, OSError)


class UnknownColumnError(EpcConnectorError, ValueError):
    """A column asked for is not in the domestic certificates."""


class NotSetUpError(EpcConnectorError, RuntimeError):
    """No data could be set up for the request."""


@overload
def load_domestic_certificates(
    origin: Origin | str | None = None,
    *,
    columns: Sequence[str] | None = None,
    lazy: Literal[False] = False,
    data_dir: Path | str | None = None,
    strict: bool = True,
    download_if_missing: bool = True,
    force_download: bool = False,
    api_token: str | None = None,
) -> pl.DataFrame: ...


@overload
def load_domestic_certificates(
    origin: Origin | str | None = None,
    *,
    columns: Sequence[str] | None = None,
    lazy: Literal[True],
    data_dir: Path | str | None = None,
    strict: bool = True,
    download_if_missing: bool = True,
    force_download: bool = False,
    api_token: str | None = None,
) -> pl.LazyFrame: ...


def load_domestic_certificates(
    origin: Origin | str | None = None,
    *,
    columns: Sequence[str] | None = None,
    lazy: bool = False,
    data_dir: Path | str | None = None,
    strict: bool = True,
    download_if_missing: bool = True,
    force_download: bool = False,
    api_token: str | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Load domestic EPCs as canonical GB records, setting them up if needed.

    Missing data is downloaded and built first, for the origins asked for
    only; a first download is several GB and announced with a warning.
    Each origin's version is then checked from the GB file's metadata
    (nothing else is read): data more than ``STALE_AFTER_MONTHS`` old comes
    with a warning suggesting ``update``.

    Parameters
    ----------
    origin
        ``"england-and-wales"`` (or ``"england"``, ``"wales"``),
        ``"scotland"``, or ``"gb"`` / ``None`` for both. For GB data, an
        origin that cannot be set up (e.g. no England and Wales API token)
        is left out with a warning instead of failing.
    columns
        Columns to load (see ``schemas/canonical/``); all if ``None``.
    lazy
        Return a ``polars.LazyFrame`` that reads nothing until collected,
        instead of a ``DataFrame``. Every column of every certificate takes
        far more memory than the file on disk: pass ``columns``, or use
        ``lazy=True`` and filter before collecting.
    data_dir
        Data directory; see ``workspace.resolve_data_dir``.
    strict
        When building GB rows, raise if a description is not covered by the
        vocabularies instead of marking it.
    download_if_missing
        Download an origin's raw data if it is not there. If false, a
        missing origin raises ``NotSetUpError`` instead (or, for GB data, is
        left out with a warning); GB rows are still rebuilt from raw files
        already present.
    force_download
        Download the raw data of the origins asked for again, even if it is
        there, then rebuild their GB rows; overrides ``download_if_missing``.
    api_token
        England and Wales API token, needed only to download or update its
        data. If ``None``: ``EPC_CONNECTOR_API_TOKEN``, then the token saved by
        the ``epc-connector`` command (see ``credentials``).

    Returns
    -------
    polars.DataFrame or polars.LazyFrame
        One row per certificate, in canonical column order.

    Raises
    ------
    UnknownColumnError
        If a column in ``columns`` does not exist.
    NotSetUpError
        If no origin asked for could be set up, or one is missing and
        ``download_if_missing`` is false.
    """
    report_unknown_variables()
    workspace = Workspace.resolve(data_dir)
    origins = parse_origins(origin)
    whole_gb = len(origins) > 1
    with lock_data_dir(workspace.data_dir):
        for each in origins:
            try:
                work = plan_domestic(
                    each,
                    workspace=workspace,
                    strict=strict,
                    api_token=api_token,
                    force=force_download,
                )
                if work.downloads:
                    _announce_or_refuse(
                        work.downloads, download_if_missing or force_download
                    )
                if work.steps:
                    run_plan(work, workspace, api_token=api_token)
            except _SETUP_ERRORS as error:
                if not whole_gb:
                    raise
                logger.warning("%s domestic data could not be set up: %s", each, error)
                logger.debug("Details of the error", exc_info=True)

    versions = {o: v for o, v in gb_origins(workspace.data_dir).items() if o in origins}
    if not versions:
        raise NotSetUpError("no domestic data could be set up; see the warnings")
    if whole_gb and len(versions) < len(origins):
        logger.warning("GB domestic data holds %s only", ", ".join(map(str, versions)))
    for each, version in versions.items():
        _warn_if_stale(each, version)

    frame = pl.scan_parquet(workspace.data_dir / GB_PARQUET_NAME, low_memory=True)
    if not whole_gb:
        name = get_dataset(origins[0], Segment.DOMESTIC).name
        frame = frame.filter(pl.col("source_dataset") == name)
    if columns is not None:
        frame = frame.select(_checked_columns(frame, columns))
    return frame if lazy else frame.collect(engine="streaming")


def update(
    origin: Origin | str | None = None,
    segments: Iterable[Segment | str] | None = None,
    *,
    date_from: dt.date | str | None = None,
    date_to: dt.date | str | None = None,
    data_dir: Path | str | None = None,
    staging_dir: Path | str | None = None,
    strict: bool = True,
    api_token: str | None = None,
) -> None:
    """
    Update downloaded datasets from their publishers, then the GB file.

    The same as ``epc-connector update``: datasets that were never
    downloaded are left out.

    Parameters
    ----------
    origin
        As for ``load_domestic_certificates``.
    segments
        ``"domestic"``, ``"non-domestic"``, ``"display"``; all if ``None``.
    date_from, date_to
        Window to refresh (dates or ``"YYYY-MM-DD"``): lodgement date for
        England and Wales, ``CREATED_AT`` date for Scotland. By default,
        from the latest date in each local file.
    data_dir, staging_dir
        See ``workspace.resolve_data_dir`` and
        ``workspace.resolve_staging_dir``.
    strict, api_token
        As for ``load_domestic_certificates``.
    """
    report_unknown_variables()
    workspace = Workspace.resolve(data_dir, staging_dir)
    with lock_data_dir(workspace.data_dir):
        work = plan_update(
            parse_origins(origin),
            None if segments is None else [Segment(segment) for segment in segments],
            date_from=date_from,
            date_to=date_to,
            workspace=workspace,
            strict=strict,
            api_token=api_token,
        )
        run_plan(work, workspace, api_token=api_token)


def status(data_dir: Path | str | None = None) -> list[DatasetStatus]:
    """
    Return which datasets and GB files are set up, from file metadata only.

    The same as ``epc-connector status``.
    """
    report_unknown_variables()
    return dataset_statuses(resolve_data_dir(data_dir))


def _announce_or_refuse(
    downloads: Sequence[Dataset], download_if_missing: bool
) -> None:
    """Warn before a first download, or refuse it if not allowed."""
    names = ", ".join(dataset.name for dataset in downloads)
    if not download_if_missing:
        raise NotSetUpError(
            f"{names} is not downloaded; pass download_if_missing=True or run "
            "epc-connector setup"
        )
    logger.warning(
        "Downloading %s: several GB, which can take an hour or more. Later "
        "loads use the local copy.",
        names,
    )


def _checked_columns(frame: pl.LazyFrame, columns: Sequence[str]) -> list[str]:
    """Return ``columns`` as a list; raise if one does not exist."""
    wanted = [columns] if isinstance(columns, str) else list(columns)
    known = set(frame.collect_schema().names())
    unknown = [column for column in wanted if column not in known]
    if unknown:
        raise UnknownColumnError(f"no such columns: {', '.join(unknown)}")
    return wanted


def _warn_if_stale(origin: Origin, version: str) -> None:
    """Warn if an origin's data runs to a date over the age limit."""
    as_of = _DATA_DATES[origin](version)
    if as_of < _months_before(dt.date.today(), STALE_AFTER_MONTHS):
        logger.warning(
            "%s domestic data runs to %s, more than %d months ago; "
            "epc_connector.update() fetches newer data",
            origin,
            as_of,
            STALE_AFTER_MONTHS,
        )


def _months_before(day: dt.date, months: int) -> dt.date:
    """Return the same day ``months`` earlier, or that month's last day."""
    year, month_index = divmod(day.year * 12 + day.month - 1 - months, 12)
    month = month_index + 1
    return dt.date(year, month, min(day.day, calendar.monthrange(year, month)[1]))
