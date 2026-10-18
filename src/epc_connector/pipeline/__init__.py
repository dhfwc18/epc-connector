"""
The ``epc-connector`` command line, kept apart from the library.

Modules
-------
cli
    Argument parsing and the ``setup``, ``update``, ``status`` and
    ``coverage`` commands.
plan
    Turns a setup or update request into ordered steps.
process
    Runs each step in a fresh process.
status
    Reports which datasets and GB files are set up.
coverage
    Reports how the vocabularies cover the raw descriptions.
"""
