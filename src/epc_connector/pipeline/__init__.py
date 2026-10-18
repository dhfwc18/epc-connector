"""
Plan and run setup and update steps, for the library and the command line.

Modules
-------
cli
    Argument parsing and the ``setup``, ``update``, ``status`` and
    ``coverage`` commands.
plan
    Turns a setup, update or load request into ordered steps.
run
    Checks a plan's prerequisites and runs its steps.
process
    Runs a step in a fresh process (the command line).
status
    Reports which datasets and GB files are set up.
logs
    Logging for the command line.
coverage
    Reports how the vocabularies cover the raw descriptions.
"""
