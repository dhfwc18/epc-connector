"""
The EPC datasets this package handles, with the facts about each in one place.

A dataset is one origin (who publishes it) and one segment (which buildings
it covers). England and Wales are published together, so they are one origin.
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from epc_connector._errors import EpcConnectorError
from epc_connector._validation import DEFAULT_SCHEMAS_DIR

logger = logging.getLogger(__name__)

RELEASE_METADATA_KEY: Final = "epc_connector.release"
"""Parquet metadata key holding the Scottish release a local copy holds."""

LAST_UPDATED_METADATA_KEY: Final = "epc_connector.last_updated"
"""Parquet metadata key holding when an England and Wales file was built."""


class Origin(StrEnum):
    """
    Publisher of a dataset.

    Attributes
    ----------
    ENGLAND_AND_WALES
        Get energy performance of buildings data (England and Wales together).
    SCOTLAND
        Scottish EPC Register extracts on data.gov.scot.
    """

    ENGLAND_AND_WALES = "england-and-wales"
    SCOTLAND = "scotland"


class Segment(StrEnum):
    """
    Kind of certificate a dataset holds.

    Attributes
    ----------
    DOMESTIC
        Energy Performance Certificates for dwellings.
    NON_DOMESTIC
        Energy Performance Certificates for other buildings.
    DISPLAY
        Display Energy Certificates for public buildings (England and Wales).
    """

    DOMESTIC = "domestic"
    NON_DOMESTIC = "non-domestic"
    DISPLAY = "display"


class DatasetError(EpcConnectorError, ValueError):
    """An origin or segment that does not exist, or a pair nobody publishes."""


@dataclass(frozen=True)
class Dataset:
    """
    One published dataset and where its local copy lives.

    Attributes
    ----------
    origin
        Publisher.
    segment
        Kind of certificate.
    parquet_name
        File name of the local copy in the data directory.
    provenance_key
        Parquet metadata key recording which release the local copy holds.
    """

    origin: Origin
    segment: Segment
    parquet_name: str
    provenance_key: str

    @property
    def name(self) -> str:
        """``"<origin>/<segment>"``, e.g. ``"scotland/domestic"``."""
        return f"{self.origin}/{self.segment}"

    def source_schema(self, schemas_dir: Path | str = DEFAULT_SCHEMAS_DIR) -> Path:
        """Return the JSON Schema that validates the raw rows."""
        return (
            Path(schemas_dir) / "source" / self.origin / f"{self.segment}.schema.json"
        )

    def path(self, data_dir: Path | str) -> Path:
        """Return the local copy's path inside ``data_dir``."""
        return Path(data_dir) / self.parquet_name


def _dataset(origin: Origin, segment: Segment, parquet_name: str) -> Dataset:
    provenance_key = (
        RELEASE_METADATA_KEY if origin is Origin.SCOTLAND else LAST_UPDATED_METADATA_KEY
    )
    return Dataset(origin, segment, parquet_name, provenance_key)


DATASETS: Final[dict[tuple[Origin, Segment], Dataset]] = {
    (dataset.origin, dataset.segment): dataset
    for dataset in [
        _dataset(
            Origin.SCOTLAND,
            Segment.NON_DOMESTIC,
            "non_domestic_epc_certificate_scotland.parquet",
        ),
        _dataset(
            Origin.SCOTLAND,
            Segment.DOMESTIC,
            "domestic_epc_certificate_scotland.parquet",
        ),
        _dataset(
            Origin.ENGLAND_AND_WALES,
            Segment.NON_DOMESTIC,
            "non_domestic_epc_certificate_england_wales.parquet",
        ),
        _dataset(
            Origin.ENGLAND_AND_WALES,
            Segment.DISPLAY,
            "display_energy_certificate_england_wales.parquet",
        ),
        _dataset(
            Origin.ENGLAND_AND_WALES,
            Segment.DOMESTIC,
            "domestic_epc_certificate_england_wales.parquet",
        ),
    ]
}
"""Every published dataset, smallest first within each origin."""

GB: Final = "gb"
"""Origin name meaning every origin."""

# Names people use for one half of the England and Wales dataset.
_ORIGIN_ALIASES: Final = {
    "england": Origin.ENGLAND_AND_WALES,
    "wales": Origin.ENGLAND_AND_WALES,
}


def get_dataset(origin: Origin | str, segment: Segment | str) -> Dataset:
    """
    Return the dataset an origin publishes for a segment.

    Raises
    ------
    DatasetError
        If the origin or segment is unknown, or the origin does not publish
        that segment (e.g. Scottish display certificates).
    """
    key = (_origin(origin), _segment(segment))
    if key not in DATASETS:
        raise DatasetError(f"{key[0]} does not publish {key[1]} certificates")
    return DATASETS[key]


def parse_origins(name: str | None) -> list[Origin]:
    """
    Return the origins a user-supplied name stands for.

    ``None`` or ``"gb"`` mean every origin. ``"england"`` and ``"wales"`` mean
    England and Wales, which are published as one dataset; a warning says so.

    Raises
    ------
    DatasetError
        If the name is not an origin.
    """
    if name is None or name == GB:
        return list(Origin)
    if name in _ORIGIN_ALIASES:
        logger.warning(
            "England and Wales are published as one dataset; using %s",
            Origin.ENGLAND_AND_WALES,
        )
        return [_ORIGIN_ALIASES[name]]
    return [_origin(name)]


def select_datasets(
    origins: Iterable[Origin], segments: Iterable[Segment] | None = None
) -> list[Dataset]:
    """
    Return the datasets of some origins and segments, in ``DATASETS`` order.

    ``segments=None`` means every segment each origin publishes. Pairs that
    nobody publishes are left out (asking for display certificates from every
    origin gives England and Wales only).

    Raises
    ------
    DatasetError
        If nothing is left, e.g. display certificates from Scotland only.
    """
    origins = set(origins)
    wanted = None if segments is None else set(segments)
    selected = [
        dataset
        for dataset in DATASETS.values()
        if dataset.origin in origins and (wanted is None or dataset.segment in wanted)
    ]
    if not selected:
        names = ", ".join(sorted(map(str, wanted or ()))) or "any"
        raise DatasetError(
            f"no {names} certificates are published for "
            f"{' or '.join(sorted(origins))}"
        )
    return selected


def _origin(value: Origin | str) -> Origin:
    try:
        return Origin(value)
    except ValueError as error:
        choices = ", ".join([*Origin, *_ORIGIN_ALIASES, GB])
        raise DatasetError(
            f"unknown origin {value!r}; choose from {choices}"
        ) from error


def _segment(value: Segment | str) -> Segment:
    try:
        return Segment(value)
    except ValueError as error:
        choices = ", ".join(Segment)
        raise DatasetError(
            f"unknown segment {value!r}; choose from {choices}"
        ) from error
