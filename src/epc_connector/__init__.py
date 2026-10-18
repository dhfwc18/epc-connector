"""
Download, update, clean and merge UK domestic Energy Performance Certificates.

Typical use, writing Parquet files to ``.epc_data/`` (see ``locations``)::

    from epc_connector import build_gb, download, replace_origin_in_gb, update

    download("scotland", "domestic")
    download("england-and-wales", "domestic")  # token in EPB_API_TOKEN
    build_gb()  # .epc_data/domestic_epc_certificate_gb.parquet

    update("scotland", "domestic", date_from="2026-07-01")
    replace_origin_in_gb("scotland")  # the England and Wales rows are kept

``clean_domestic_epc`` returns one origin's raw file with its descriptions
cleaned, as a lazy frame. Every error the package raises on purpose is an
``EpcConnectorError``. The ``epc-connector`` command line wraps these
functions; see ``epc_connector.pipeline``.
"""

from epc_connector.canonical import build_gb, replace_origin_in_gb
from epc_connector.cleaning import clean_domestic_epc
from epc_connector.datasets import Origin, Segment
from epc_connector.errors import EpcConnectorError
from epc_connector.sources import download, update

__all__ = [
    "EpcConnectorError",
    "Origin",
    "Segment",
    "build_gb",
    "clean_domestic_epc",
    "download",
    "replace_origin_in_gb",
    "update",
]
