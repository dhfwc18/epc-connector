"""
Run each pipeline step in a fresh process.

Polars and Python keep freed memory reserved inside a process, so running
several large steps in one process stacks their peaks. A separate process
hands all of a step's memory back to the operating system when it ends.
"""

import logging
import multiprocessing
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

LOG_FORMAT = "%(asctime)s %(message)s"
"""Log line format used by the command line and its step processes."""


def configure_logging() -> None:
    """Log ``INFO`` and above to stderr with ``LOG_FORMAT``."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def run_step(function: Callable[..., Path], /, *args: Any, **kwargs: Any) -> Path:
    """
    Run ``function(*args, **kwargs)`` in a fresh process and return its result.

    The function and its arguments must be picklable (module-level functions
    and plain values). An error in the step is raised here.
    """
    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=configure_logging,
    ) as pool:
        return pool.submit(function, *args, **kwargs).result()
