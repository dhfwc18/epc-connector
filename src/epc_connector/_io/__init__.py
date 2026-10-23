"""
Download, unpack and convert raw source files with bounded memory.

Internal: not part of the public API (see ``epc_connector``).

Modules
-------
files
    Page-cache-friendly writes, staging directories, atomic replacement.
downloads
    HTTP downloads that resume, and range reads of remote files.
archives
    Extraction of selected members from 7z and zip archives.
parquet
    CSV to Parquet conversion and streaming Parquet writes.
"""

from epc_connector._io.archives import extract_files, extract_remote_zip_members
from epc_connector._io.downloads import (
    HTTP_TIMEOUT,
    RemoteFile,
    download_file,
    resolve_download_url,
)
from epc_connector._io.files import (
    StorageError,
    StreamingFile,
    atomic_write,
    drop_page_cache,
    staging_directory,
)
from epc_connector._io.parquet import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    archive_url_to_parquet,
    merge_csvs_to_parquet,
    read_csv_header,
    stream_to_parquet,
)

__all__ = [
    "DEFAULT_CHUNK_ROWS",
    "DEFAULT_ROW_GROUP_ROWS",
    "HTTP_TIMEOUT",
    "RemoteFile",
    "StorageError",
    "StreamingFile",
    "archive_url_to_parquet",
    "atomic_write",
    "download_file",
    "drop_page_cache",
    "extract_files",
    "extract_remote_zip_members",
    "merge_csvs_to_parquet",
    "read_csv_header",
    "resolve_download_url",
    "staging_directory",
    "stream_to_parquet",
]
