"""
Load UK domestic Energy Performance Certificates as one Great Britain dataset.

Typical use::

    import os

    import epc_connector as epc

    homes = epc.load_domestic_certificates(
        "scotland", columns=["uprn", "current_energy_rating"]
    )
    # Keeping the token in a .env file? Load it first (python-dotenv):
    # from dotenv import load_dotenv; load_dotenv()
    token = os.environ["EPB_API_TOKEN"]
    homes = epc.load_domestic_certificates(api_token=token)
    epc.update(api_token=token)  # fetch newer data
    epc.status()  # what is set up

The first load downloads and builds what it needs (several GB, into
``.epc_data/``; see ``locations``). Only England and Wales data needs the
API token; without ``api_token`` it is found as ``credentials`` describes.
The ``epc-connector`` command does the same from a shell. Every error the
package raises on purpose is an ``EpcConnectorError``.

Only the names in ``__all__`` below are the public API. Submodules such as
``sources``, ``storage``, ``cleaning``, ``canonical``, ``pipeline`` and
``cli`` are internal and may change in any release.
"""

from epc_connector.api import (
    NotSetUpError,
    UnknownColumnError,
    load_domestic_certificates,
    status,
    update,
)
from epc_connector.credentials import ApiTokenError
from epc_connector.datasets import Origin, Segment
from epc_connector.errors import EpcConnectorError
from epc_connector.pipeline.status import DatasetStatus

__all__ = [
    "ApiTokenError",
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
