"""
Download, update, clean and merge UK domestic Energy Performance Certificates.

Typical use, writing Parquet files to ``data/``::

    from epc_connector import (
        download_england_and_wales_epc,
        download_scotland_epc,
        merge_gb_domestic_epc,
    )

    download_scotland_epc("domestic")
    download_england_and_wales_epc("domestic")  # token in EPB_API_TOKEN
    merge_gb_domestic_epc()  # data/domestic_epc_certificate_gb.parquet

``update_scotland_epc`` and ``update_england_and_wales_epc`` refresh a
downloaded file; ``clean_domestic_epc`` returns one nation's raw file with its
descriptions cleaned, as a lazy frame.
"""

from epc_connector.cleaning import Nation, clean_domestic_epc
from epc_connector.external.england_wales_epc import (
    EnglandWalesEpcDataset,
    download_england_and_wales_epc,
    update_england_and_wales_epc,
)
from epc_connector.external.scotland_epc import (
    ScotlandEpcDataset,
    download_scotland_epc,
    update_scotland_epc,
)
from epc_connector.merge import merge_gb_domestic_epc

__all__ = [
    "EnglandWalesEpcDataset",
    "Nation",
    "ScotlandEpcDataset",
    "clean_domestic_epc",
    "download_england_and_wales_epc",
    "download_scotland_epc",
    "merge_gb_domestic_epc",
    "update_england_and_wales_epc",
    "update_scotland_epc",
]
