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
    token = os.environ["EPC_CONNECTOR_API_TOKEN"]
    homes = epc.load_domestic_certificates(api_token=token)
    epc.update(api_token=token)  # fetch newer data
    epc.status()  # what is set up

The first load downloads and builds what it needs (several GB, into the
per-user data directory; see ``_workspace``). Only England and Wales data needs
the API token; without ``api_token`` it is found as ``_credentials`` describes.
The ``epc-connector`` command does the same from a shell. Every error the
package raises on purpose is an ``EpcConnectorError``.

Only the names in ``__all__`` below are the public API. Everything else
lives in underscore-prefixed modules (``_sources``, ``_io``, ``_cli`` and
so on), which are internal, as PEP 8 recommends, and may change in any
release.
"""

from epc_connector._api import (
    NotSetUpError,
    UnknownColumnError,
    load_domestic_certificates,
    status,
    update,
)
from epc_connector._credentials import ApiTokenError
from epc_connector._datasets import Origin, Segment
from epc_connector._errors import EpcConnectorError
from epc_connector._pipeline.status import DatasetStatus

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
