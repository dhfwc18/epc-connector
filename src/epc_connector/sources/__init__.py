"""
Download and update EPC data from each publisher.

Internal: not part of the public API (see ``epc_connector``).

Each origin has a module with the same two functions, ``download(segment)``
and ``update(segment)``; ``download`` and ``update`` here pick the module for
an origin.

Modules
-------
scotland
    Scottish EPC Register extracts on data.gov.scot (CKAN).
england_and_wales
    Get energy performance of buildings data (bulk files and API).
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

from epc_connector.datasets import Origin, Segment, get_dataset
from epc_connector.sources import england_and_wales, scotland

_DOWNLOADS: Final[dict[Origin, Callable[..., Path]]] = {
    Origin.ENGLAND_AND_WALES: england_and_wales.download,
    Origin.SCOTLAND: scotland.download,
}
_UPDATES: Final[dict[Origin, Callable[..., Path]]] = {
    Origin.ENGLAND_AND_WALES: england_and_wales.update,
    Origin.SCOTLAND: scotland.update,
}


def download(origin: Origin | str, segment: Segment | str, **options: Any) -> Path:
    """
    Download one dataset into a Parquet file.

    Parameters
    ----------
    origin, segment
        Dataset to download, e.g. ``"scotland"`` and ``"domestic"``.
    **options
        Passed on to the origin module's ``download`` (``data_dir``,
        ``staging_dir``, ...).

    Returns
    -------
    Path
        Path of the Parquet file.

    Raises
    ------
    DatasetError
        If the origin does not publish the segment.
    """
    dataset = get_dataset(origin, segment)
    return _DOWNLOADS[dataset.origin](dataset.segment, **options)


def update(origin: Origin | str, segment: Segment | str, **options: Any) -> Path:
    """
    Update one downloaded dataset from its publisher.

    Parameters
    ----------
    origin, segment
        Dataset to update, e.g. ``"scotland"`` and ``"domestic"``.
    **options
        Passed on to the origin module's ``update`` (``date_from``,
        ``date_to``, ``data_dir``, ...).

    Returns
    -------
    Path
        Path of the updated Parquet file.

    Raises
    ------
    DatasetError
        If the origin does not publish the segment.
    """
    dataset = get_dataset(origin, segment)
    return _UPDATES[dataset.origin](dataset.segment, **options)


__all__ = ["download", "england_and_wales", "scotland", "update"]
