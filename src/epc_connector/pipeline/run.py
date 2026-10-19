"""
Run a plan: check what it needs, then carry out its steps in order.

The command line runs each step in a fresh process (``process.run_step``),
so its memory goes back to the operating system when it ends. Library calls
run steps in the calling process instead: a spawned process re-imports the
caller's main script, which would need an ``if __name__ == "__main__":``
guard in every script that loads data.
"""

import datetime as dt
import logging
import shutil
import time
from pathlib import Path
from typing import Final

from epc_connector.credentials import require_api_token
from epc_connector.datasets import Origin, Segment
from epc_connector.pipeline import process
from epc_connector.pipeline.logs import DEFAULT_LOG_OPTIONS, LogOptions
from epc_connector.pipeline.plan import Plan, Workspace
from epc_connector.storage import StorageError

logger = logging.getLogger(__name__)

MIN_FREE_STAGING_BYTES: Final = 10 * 1024**3
"""Free staging space a Scottish domestic run needs (about 8 GB, plus room)."""


def run_plan(
    work: Plan,
    workspace: Workspace,
    *,
    isolated: bool = False,
    log_options: LogOptions = DEFAULT_LOG_OPTIONS,
    api_token: str | None = None,
) -> None:
    """
    Check a plan's prerequisites, then run its steps in order.

    Parameters
    ----------
    work
        Plan to run.
    workspace
        Directories the plan was made for.
    isolated
        Run each step in a fresh process (the command line) rather than in
        this one.
    log_options
        How isolated steps log; see ``logs.configure_logging``.
    api_token
        England and Wales API token to check for; if ``None``, one is found
        as ``credentials`` describes.

    Raises
    ------
    StorageError
        If a Scottish domestic download lacks staging space.
    ApiTokenError
        If an England and Wales step has no API token.
    """
    logger.info("Data directory: %s", workspace.data_dir.resolve())
    for dataset in work.skipped:
        logger.info(
            "%s is already downloaded; update refreshes it, "
            "setup --force downloads it again",
            dataset.name,
        )
    for dataset in work.not_downloaded:
        logger.warning("%s is not downloaded, so not updated", dataset.name)
    if not work.steps:
        logger.info("Nothing to do")
        return
    if any(
        dataset.origin is Origin.SCOTLAND and dataset.segment is Segment.DOMESTIC
        for dataset in work.datasets
    ):
        check_staging_space(workspace.staging_dir)
    if any(dataset.origin is Origin.ENGLAND_AND_WALES for dataset in work.datasets):
        require_api_token(api_token)  # fail before any download without one

    started = time.perf_counter()
    for number, step in enumerate(work.steps, start=1):
        logger.info("Step %d of %d: %s", number, len(work.steps), step.name)
        step_started = time.perf_counter()
        if isolated:
            path = process.run_step(
                step.function, *step.args, log_options=log_options, **step.kwargs
            )
        else:
            path = step.function(*step.args, **step.kwargs)
        logger.debug("Step %d done in %s: %s", number, _elapsed(step_started), path)
    logger.info("Finished %d steps in %s", len(work.steps), _elapsed(started))
    for segment in work.not_set_up:
        logger.warning(
            "GB %s is not set up yet; only the raw %s files were changed",
            segment,
            segment,
        )


def check_staging_space(staging_dir: Path) -> None:
    """
    Raise unless there is room for a Scottish domestic download.

    Raises
    ------
    StorageError
        If less than ``MIN_FREE_STAGING_BYTES`` is free under
        ``staging_dir`` (or its nearest existing parent).
    """
    staging = staging_dir.resolve()
    existing = next(p for p in [staging, *staging.parents] if p.exists())
    free = shutil.disk_usage(existing).free
    if free < MIN_FREE_STAGING_BYTES:
        raise StorageError(
            f"only {free / 1024**3:.1f} GB free under {staging_dir}; "
            "Scottish domestic needs about 10 GB"
        )


def _elapsed(started: float) -> str:
    """Time since ``started`` (a ``time.perf_counter`` value) as H:MM:SS."""
    return str(dt.timedelta(seconds=round(time.perf_counter() - started)))
