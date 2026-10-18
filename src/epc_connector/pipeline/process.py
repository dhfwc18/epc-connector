"""
Run each pipeline step in a fresh process.

Polars and Python keep freed memory reserved inside a process, so running
several large steps in one process stacks their peaks. A separate process
hands all of a step's memory back to the operating system when it ends.
"""

import multiprocessing
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

from epc_connector.pipeline.logs import (
    DEFAULT_LOG_OPTIONS,
    LogOptions,
    configure_logging,
)


def run_step(
    function: Callable[..., Path],
    /,
    *args: Any,
    log_options: LogOptions = DEFAULT_LOG_OPTIONS,
    **kwargs: Any,
) -> Path:
    """
    Run ``function(*args, **kwargs)`` in a fresh process and return its result.

    The function and its arguments must be picklable (module-level functions
    and plain values). The process logs with ``log_options``, like the
    command line itself. An error in the step is raised here.
    """
    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=configure_logging,
        initargs=(log_options,),
    ) as pool:
        return pool.submit(function, *args, **kwargs).result()
