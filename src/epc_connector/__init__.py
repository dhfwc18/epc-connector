"""
Load UK domestic Energy Performance Certificates as one Great Britain dataset.

Typical use::

    import epc_connector as epc

    homes = epc.load_domestic_certificates(
        "scotland", columns=["uprn", "current_energy_rating"]
    )
    epc.update()  # fetch newer data
    epc.status()  # what is set up

The first load downloads and builds what it needs (several GB, into
``.epc_data/``; see ``locations``). The ``epc-connector`` command does the
same from a shell. Every error the package raises on purpose is an
``EpcConnectorError``.
"""

from epc_connector.api import (
    NotSetUpError,
    UnknownColumnError,
    load_domestic_certificates,
    status,
    update,
)
from epc_connector.datasets import Origin, Segment
from epc_connector.errors import EpcConnectorError
from epc_connector.pipeline.status import DatasetStatus

__all__ = [
    "DatasetStatus",
    "EpcConnectorError",
    "NotSetUpError",
    "Origin",
    "Segment",
    "UnknownColumnError",
    "load_domestic_certificates",
    "status",
    "update",
]
