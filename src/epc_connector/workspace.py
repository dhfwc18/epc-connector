"""
The workspace: where data is kept, staged and locked, and memory limits.

``Workspace`` bundles the directories and limits a plan runs with;
``Workspace.resolve`` fills in the defaults below.

Downloads go to ``.epc_data/`` in the working directory, so each project
keeps its own copy next to its ``.venv`` and a second ``setup`` finds it
there. Set ``EPC_CONNECTOR_DATA_DIR`` to share one copy between projects.

When the package creates a data directory it adds a ``.gitignore`` holding
``*`` and a ``CACHEDIR.TAG``, so version control and backup tools skip it,
as with ``.pytest_cache``. Existing directories and files are left as they
are. Runs that download or rebuild files hold the directory's lock
(``lock_data_dir``), so two runs never fetch the same data at once.
"""

import contextlib
import logging
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from filelock import FileLock, Timeout

from epc_connector.environment import EnvVar, read_path
from epc_connector.io import DEFAULT_CHUNK_ROWS, DEFAULT_ROW_GROUP_ROWS

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR: Final = Path(".epc_data")
"""Data directory, relative to the working directory, if none is given."""
LOCK_FILE_NAME: Final = ".lock"
"""Lock file inside the data directory; see ``lock_data_dir``."""
STAGING_DIR_NAME: Final = ".staging"
"""Default staging directory, inside the data directory."""
GITIGNORE_TEXT: Final = "# Created by epc-connector: downloaded data.\n*\n"
"""Contents of the ``.gitignore`` written into a new data directory."""
CACHEDIR_TAG_TEXT: Final = (
    "Signature: 8a477f597d28d172789f06886806bc55\n"
    "# This file is a cache directory tag created by epc-connector.\n"
    "# For information about cache directory tags, see:\n"
    "#\thttps://bford.info/cachedir/\n"
)
"""Contents of the ``CACHEDIR.TAG`` written into a new data directory."""


def resolve_data_dir(data_dir: Path | str | None = None) -> Path:
    """
    Return the data directory to use.

    In order: ``data_dir`` if given, then ``EPC_CONNECTOR_DATA_DIR`` if set,
    then ``.epc_data`` in the working directory. ``~`` is expanded.
    """
    if data_dir is not None:
        return Path(data_dir).expanduser()
    return read_path(EnvVar.DATA_DIR) or DEFAULT_DATA_DIR


def resolve_staging_dir(staging_dir: Path | str | None, data_dir: Path) -> Path:
    """
    Return ``staging_dir``, or ``.staging`` inside ``data_dir`` if ``None``.

    Inside the data directory, staging is on the same disk, so finished
    files are moved into place by a rename.
    """
    if staging_dir is None:
        return data_dir / STAGING_DIR_NAME
    return Path(staging_dir).expanduser()


@dataclass(frozen=True)
class Workspace:
    """
    Where the pipeline reads and writes, and its memory limits.

    Build one with ``Workspace.resolve`` to apply the default directories.

    Attributes
    ----------
    data_dir
        Directory of the raw and GB Parquet files.
    staging_dir
        Parent of the temporary directories used while writing.
    chunk_rows, row_group_rows
        Memory limits; see ``io.stream_to_parquet``.
    """

    data_dir: Path
    staging_dir: Path
    chunk_rows: int = DEFAULT_CHUNK_ROWS
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS

    @classmethod
    def resolve(
        cls,
        data_dir: Path | str | None = None,
        staging_dir: Path | str | None = None,
        *,
        chunk_rows: int = DEFAULT_CHUNK_ROWS,
        row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    ) -> Self:
        """
        Return a workspace, filling in the default directories.

        See ``resolve_data_dir`` and ``resolve_staging_dir``.
        """
        resolved = resolve_data_dir(data_dir)
        return cls(
            data_dir=resolved,
            staging_dir=resolve_staging_dir(staging_dir, resolved),
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
        )

    def options(self) -> dict[str, Any]:
        """Keyword arguments every download, update and GB function takes."""
        return {
            "data_dir": self.data_dir,
            "staging_dir": self.staging_dir,
            "chunk_rows": self.chunk_rows,
            "row_group_rows": self.row_group_rows,
        }


def prepare_data_dir(data_dir: Path) -> Path:
    """
    Create ``data_dir`` if it is missing, marked as ignorable; return it.

    A new directory gets a ``.gitignore`` holding ``*`` and a
    ``CACHEDIR.TAG``. An existing directory is not changed.
    """
    try:
        data_dir.mkdir(parents=True)
    except FileExistsError:
        return data_dir
    for name, text in [
        (".gitignore", GITIGNORE_TEXT),
        ("CACHEDIR.TAG", CACHEDIR_TAG_TEXT),
    ]:
        with (data_dir / name).open("x", encoding="ascii") as marker:
            marker.write(text)
    return data_dir


@contextlib.contextmanager
def lock_data_dir(data_dir: Path) -> Generator[Path]:
    """
    Hold the data directory's lock while setting it up or updating it.

    Take the lock before planning, not just before running: a second run
    then waits for the first, plans from the files it left and downloads
    nothing twice. The operating system releases the lock if a process
    dies, so it never goes stale. The directory is created if missing (see
    ``prepare_data_dir``).
    """
    prepare_data_dir(data_dir)
    lock = FileLock(data_dir / LOCK_FILE_NAME)
    try:
        lock.acquire(blocking=False)
    except Timeout:
        logger.warning(
            "Another epc-connector run is using %s; waiting for it to finish",
            data_dir,
        )
        lock.acquire()
    try:
        yield data_dir
    finally:
        lock.release()
