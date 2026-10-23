"""
The ``epc-connector`` command line, kept apart from the library.

Internal: not part of the public API (see ``epc_connector``).

Importing ``epc_connector`` does not load this package. It drives the same
plans as the library (``pipeline``), but runs each step in a fresh process.

Modules
-------
commands
    Argument parsing and the ``setup``, ``update``, ``status``,
    ``coverage``, ``login`` and ``logout`` commands.
process
    Runs a step in a fresh process.
logs
    Console and log-file setup, shared with the step processes.
coverage
    Reports how the vocabularies cover the raw descriptions.
"""

from epc_connector.cli.commands import main

__all__ = ["main"]
