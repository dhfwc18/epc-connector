"""
Download, update, clean and merge UK domestic Energy Performance Certificates.

Typical use, writing Parquet files to ``data/``::

    from epc_connector import build_gb, download

    download("scotland", "domestic")
    download("england-and-wales", "domestic")  # token in EPB_API_TOKEN
    build_gb()  # data/domestic_epc_certificate_gb.parquet

``update(origin, segment)`` refreshes a downloaded file;
``clean_domestic_epc`` returns one origin's raw file with its descriptions
cleaned, as a lazy frame.
"""

from epc_connector.canonical import build_gb, replace_origin_in_gb
from epc_connector.cleaning import clean_domestic_epc
from epc_connector.datasets import Origin, Segment
from epc_connector.sources import download, update

__all__ = [
    "Origin",
    "Segment",
    "build_gb",
    "clean_domestic_epc",
    "download",
    "replace_origin_in_gb",
    "update",
]
