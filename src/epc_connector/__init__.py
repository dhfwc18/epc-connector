"""
Download, update, clean and merge UK domestic Energy Performance Certificates.

Typical use, writing Parquet files to ``data/``::

    from epc_connector import download, merge_gb_domestic_epc

    download("scotland", "domestic")
    download("england-and-wales", "domestic")  # token in EPB_API_TOKEN
    merge_gb_domestic_epc()  # data/domestic_epc_certificate_gb.parquet

``update(origin, segment)`` refreshes a downloaded file;
``clean_domestic_epc`` returns one nation's raw file with its descriptions
cleaned, as a lazy frame.
"""

from epc_connector.cleaning import Nation, clean_domestic_epc
from epc_connector.datasets import Origin, Segment
from epc_connector.merge import merge_gb_domestic_epc
from epc_connector.sources import download, update

__all__ = [
    "Nation",
    "Origin",
    "Segment",
    "clean_domestic_epc",
    "download",
    "merge_gb_domestic_epc",
    "update",
]
