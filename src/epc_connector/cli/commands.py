"""
Command line interface: ``epc-connector`` (or ``python -m epc_connector``).

Subcommands::

    epc-connector setup [--origin O] [--segment S ...] [--force] [--lenient]
    epc-connector update [--origin O] [--segment S ...] [--date-from D]
                         [--date-to D] [--lenient]
    epc-connector status
    epc-connector coverage [--origin O] [--element E ...]
    epc-connector login
    epc-connector logout

``--origin`` is ``england-and-wales`` (``england`` and ``wales`` mean the
same), ``scotland`` or ``gb`` (both, the default). ``setup`` downloads the
selected datasets that are not there yet (all with ``--force``) and
``update`` refreshes them; both then bring the GB file up to date. Data goes
to ``--data-dir``, else ``$EPC_CONNECTOR_DATA_DIR``, else ``./.epc_data``.

England and Wales data needs an API token: ``EPB_API_TOKEN`` if set (for CI
and scripts), else the saved token. If a command needs one and none is
found, it asks for it once (input hidden), checks it with the API and saves
it for later runs (see ``credentials``). ``login`` asks for a new token;
``logout`` deletes the saved one. Without a terminal to ask in, the command
fails instead.

Every command takes ``--debug`` (show every log message) and ``--log-file
PATH`` (record every message in a file); see ``cli.logs``.

Each download, update and GB step runs in a fresh process, so its memory is
returned to the operating system when it ends.
"""

import argparse
import functools
import getpass
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import httpx

from epc_connector import cleaning
from epc_connector.cli import coverage, logs, process
from epc_connector.credentials import (
    checked_api_token,
    delete_api_token,
    resolve_api_token,
    save_api_token,
    token_path,
)
from epc_connector.datasets import Origin, Segment, parse_origins
from epc_connector.errors import EpcConnectorError
from epc_connector.pipeline import plan, run, status
from epc_connector.sources.england_and_wales import verify_api_token
from epc_connector.workspace import (
    DATA_DIR_ENV_VAR,
    Workspace,
    lock_data_dir,
    resolve_data_dir,
)

logger = logging.getLogger(__name__)

TOKEN_URL: Final = "https://get-energy-performance-data.communities.gov.uk/"
"""Where to get an England and Wales API token (GOV.UK One Login)."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line interface; return the exit status."""
    args = build_parser().parse_args(argv)
    logs.configure_logging(_log_options(args))
    logger.debug("epc-connector %s", " ".join(sys.argv[1:] if argv is None else argv))
    try:
        return args.run(args)
    except (EpcConnectorError, FileNotFoundError, httpx.HTTPError) as error:
        logger.error("%s", error)
        logger.debug("Details of the error", exc_info=True)
        return 1
    except KeyboardInterrupt:
        logger.error("Cancelled")
        return 130


def build_parser() -> argparse.ArgumentParser:
    """Return the parser for every subcommand."""
    logging_options = argparse.ArgumentParser(add_help=False)
    logging_options.add_argument(
        "--debug",
        action="store_true",
        help="show every log message, with the process and module it came from",
    )
    logging_options.add_argument(
        "--log-file",
        type=Path,
        metavar="PATH",
        help="append every log message, including debug ones, to this file",
    )
    common = argparse.ArgumentParser(add_help=False, parents=[logging_options])
    common.add_argument(
        "--data-dir",
        type=Path,
        help=f"data directory (default: ${DATA_DIR_ENV_VAR}, else ./.epc_data)",
    )
    origin = argparse.ArgumentParser(add_help=False)
    origin.add_argument(
        "--origin",
        help="england-and-wales (or england, wales), scotland or gb (default)",
    )
    writing = argparse.ArgumentParser(add_help=False, parents=[common, origin])
    writing.add_argument(
        "--segment",
        nargs="+",
        type=Segment,
        choices=list(Segment),
        metavar="SEGMENT",
        help="domestic, non-domestic or display (default: all, smallest first)",
    )
    writing.add_argument(
        "--staging-dir",
        type=Path,
        help="scratch space for downloads (default: <data dir>/.staging)",
    )
    writing.add_argument("--chunk-rows", type=int, default=10_000)
    writing.add_argument("--row-group-rows", type=int, default=50_000)
    writing.add_argument(
        "--lenient",
        action="store_true",
        help="mark descriptions the vocabularies do not cover instead of failing",
    )

    parser = argparse.ArgumentParser(
        prog="epc-connector",
        description="Download, update, clean and merge UK EPC data.",
    )
    commands = parser.add_subparsers(required=True, metavar="command")

    setup = commands.add_parser(
        "setup",
        parents=[writing],
        help="download datasets and build the GB file",
        description=(
            "Download the selected datasets (all by default) that are not "
            "downloaded yet, then bring the GB file up to date."
        ),
    )
    setup.add_argument(
        "--force",
        action="store_true",
        help="download again datasets that are already downloaded",
    )
    setup.set_defaults(run=_setup)

    update = commands.add_parser(
        "update",
        parents=[writing],
        help="update downloaded datasets and the GB file",
        description=(
            "Update the selected downloaded datasets from their publishers, "
            "then bring the GB file up to date."
        ),
    )
    update.add_argument(
        "--date-from",
        help="first date to refresh (lodgement date for England and Wales, "
        "CREATED_AT date for Scotland; default: the latest in the local file)",
    )
    update.add_argument("--date-to", help="last date to refresh")
    update.set_defaults(run=_update)

    status_command = commands.add_parser(
        "status",
        parents=[common],
        help="list which datasets and GB files are set up",
        description="List which datasets and GB files exist in --data-dir.",
    )
    status_command.set_defaults(run=_status)

    coverage_command = commands.add_parser(
        "coverage",
        parents=[common, origin],
        help="report how the vocabularies cover the raw descriptions",
        description="Report vocabulary coverage of the raw domestic descriptions.",
    )
    coverage_command.add_argument(
        "--element",
        nargs="+",
        type=cleaning.Element,
        choices=list(cleaning.Element),
        metavar="ELEMENT",
        help="described elements, e.g. walls (default: all)",
    )
    coverage_command.set_defaults(run=_coverage)

    login = commands.add_parser(
        "login",
        parents=[logging_options],
        help="save an England and Wales API token for later runs",
        description=(
            "Ask for an England and Wales API token (input hidden), check it "
            f"with the API and save it to {token_path()}."
        ),
    )
    login.set_defaults(run=_login)

    logout = commands.add_parser(
        "logout",
        parents=[logging_options],
        help="delete the saved England and Wales API token",
        description="Delete the England and Wales API token saved by login.",
    )
    logout.set_defaults(run=_logout)
    return parser


def _setup(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    with lock_data_dir(workspace.data_dir):
        work = plan.plan_setup(
            parse_origins(args.origin),
            args.segment,
            workspace=workspace,
            strict=not args.lenient,
            force=args.force,
        )
        _ensure_api_token(work)
        run.run_plan(work, workspace, run_step=_fresh_process_runner(args))
    return 0


def _update(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    with lock_data_dir(workspace.data_dir):
        work = plan.plan_update(
            parse_origins(args.origin),
            args.segment,
            date_from=args.date_from,
            date_to=args.date_to,
            workspace=workspace,
            strict=not args.lenient,
        )
        _ensure_api_token(work)
        run.run_plan(work, workspace, run_step=_fresh_process_runner(args))
    return 0


def _status(args: argparse.Namespace) -> int:
    print("\n".join(status.status_lines(resolve_data_dir(args.data_dir))))
    return 0


def _coverage(args: argparse.Namespace) -> int:
    elements = args.element or list(cleaning.Element)
    data_dir = resolve_data_dir(args.data_dir)
    lines = coverage.coverage_lines(data_dir, parse_origins(args.origin), elements)
    print("\n".join(lines))
    return 0


def _login(args: argparse.Namespace) -> int:
    _ask_and_save_api_token()
    return 0


def _logout(args: argparse.Namespace) -> int:
    if delete_api_token():
        logger.info("Deleted the saved API token (%s)", token_path())
    else:
        logger.info("No saved API token to delete")
    if resolve_api_token() is not None:
        logger.warning("A token is still set through EPB_API_TOKEN")
    return 0


def _ensure_api_token(work: plan.Plan) -> None:
    """Ask for a token and save it if the plan needs one and none is found."""
    needs_token = any(
        dataset.origin is Origin.ENGLAND_AND_WALES for dataset in work.datasets
    )
    if needs_token and resolve_api_token() is None and _can_ask():
        _ask_and_save_api_token()


def _can_ask() -> bool:
    """Whether someone is at a terminal to type a token."""
    return sys.stdin.isatty()


def _ask_and_save_api_token() -> None:
    """Ask for a token, check it with the API, then save it."""
    logger.info(
        "England and Wales data needs an API token: sign in at %s to get one",
        TOKEN_URL,
    )
    token = checked_api_token(getpass.getpass("API token (input hidden): "))
    verify_api_token(token)
    logger.info("Token accepted; saved to %s", save_api_token(token))


def _fresh_process_runner(args: argparse.Namespace) -> run.StepRunner:
    """Run each step in a fresh process that logs like this one."""
    return functools.partial(process.run_step, log_options=_log_options(args))


def _log_options(args: argparse.Namespace) -> logs.LogOptions:
    return logs.LogOptions(debug=args.debug, log_file=args.log_file)


def _workspace(args: argparse.Namespace) -> Workspace:
    return Workspace.resolve(
        args.data_dir,
        args.staging_dir,
        chunk_rows=args.chunk_rows,
        row_group_rows=args.row_group_rows,
    )
