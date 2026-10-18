"""
Logging for the command line: what the user sees, and the full record.

The package logs at three levels:

- ``WARNING``: something the user should know about or act on (a retry, a
  dataset left out, descriptions the vocabularies do not cover).
- ``INFO``: one line per milestone (each step, each file written).
- ``DEBUG``: the detail behind them (archive members, API pages, temporary
  files, every HTTP request).

By default the console shows the package's ``INFO`` lines and warnings from
any library. ``--debug`` shows everything, with the process and module of
each line. ``--log-file`` records everything, whatever the console shows.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import httpx

PACKAGE_LOGGER: Final = "epc_connector"
"""Parent logger of every module in the package."""
CONSOLE_FORMAT: Final = "%(asctime)s %(levelname)-7s %(message)s"
"""Console line format without ``--debug``."""
CONSOLE_DATE_FORMAT: Final = "%H:%M:%S"
"""Console time format without ``--debug``."""
DETAILED_FORMAT: Final = (
    "%(asctime)s %(levelname)-7s %(processName)s %(name)s: %(message)s"
)
"""Line format of ``--debug`` and of the log file."""
QUIET_LOGGERS: Final = ("httpcore",)
"""Libraries whose ``DEBUG`` lines are too low-level even for ``--debug``."""
CONSOLE_HANDLER: Final = "epc_connector.console"
"""Name of the console handler ``configure_logging`` adds."""
FILE_HANDLER: Final = "epc_connector.log_file"
"""Name of the log file handler ``configure_logging`` adds."""


@dataclass(frozen=True)
class LogOptions:
    """
    How the command line logs; passed on to every step process.

    Attributes
    ----------
    debug
        Show every message on the console, with its process and module.
    log_file
        File that records every message (appended to), or ``None``.
    """

    debug: bool = False
    log_file: Path | None = None


DEFAULT_LOG_OPTIONS: Final = LogOptions()
"""Package milestones and all warnings on the console; no log file."""


def configure_logging(options: LogOptions = DEFAULT_LOG_OPTIONS) -> None:
    """
    Send log messages to stderr and, if asked, to a log file.

    Safe to call again: the handlers of the previous call are replaced, and
    handlers added by others (e.g. pytest) are left alone.
    """
    root = logging.getLogger()
    ours = (CONSOLE_HANDLER, FILE_HANDLER)
    for handler in [h for h in root.handlers if h.get_name() in ours]:
        root.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler()
    console.set_name(CONSOLE_HANDLER)
    if options.debug:
        console.setLevel(logging.DEBUG)
        console.setFormatter(logging.Formatter(DETAILED_FORMAT))
    else:
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(CONSOLE_FORMAT, CONSOLE_DATE_FORMAT))
        console.addFilter(_package_or_warning)
    handlers: list[logging.Handler] = [console]
    if options.log_file is not None:
        options.log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file = logging.FileHandler(options.log_file, encoding="utf-8")
        log_file.set_name(FILE_HANDLER)
        log_file.setLevel(logging.DEBUG)
        log_file.setFormatter(logging.Formatter(DETAILED_FORMAT))
        handlers.append(log_file)

    for handler in handlers:
        handler.addFilter(_hide_url_signatures)
        root.addHandler(handler)
    detailed = options.debug or options.log_file is not None
    root.setLevel(logging.DEBUG if detailed else logging.INFO)
    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.INFO)


def _package_or_warning(record: logging.LogRecord) -> bool:
    """Keep the package's messages and every library's warnings."""
    in_package = record.name == PACKAGE_LOGGER or record.name.startswith(
        f"{PACKAGE_LOGGER}."
    )
    return in_package or record.levelno >= logging.WARNING


def _hide_url_signatures(record: logging.LogRecord) -> bool:
    """
    Drop the query of pre-signed URLs, which grant access to a file.

    httpx logs each request's URL; a log file is often shared.
    """
    if isinstance(record.args, tuple):
        record.args = tuple(
            (
                arg.copy_with(query=None)
                if isinstance(arg, httpx.URL) and "X-Amz-Signature" in arg.params
                else arg
            )
            for arg in record.args
        )
    return True
