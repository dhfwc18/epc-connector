"""
Map raw rows to the canonical GB record and build the GB file.

Internal: not part of the public API (see ``epc_connector``).

Modules
-------
mapping
    Applies the ``schemas/mappings/`` files to source rows.
gb
    Builds the GB domestic file, or replaces one origin's rows in it.
"""

from epc_connector._canonical.gb import (
    GB_PARQUET_NAME,
    GB_SEGMENTS,
    GbNotSetUpError,
    build_gb,
    canonical_records,
    gb_origins,
    replace_origin_in_gb,
    require_gb_segment,
)
from epc_connector._canonical.mapping import MappingError

__all__ = [
    "GB_PARQUET_NAME",
    "GB_SEGMENTS",
    "GbNotSetUpError",
    "MappingError",
    "build_gb",
    "canonical_records",
    "gb_origins",
    "replace_origin_in_gb",
    "require_gb_segment",
]
