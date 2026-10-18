"""
Command line interface: ``epc-connector`` (or ``python -m epc_connector``).

Subcommands::

    epc-connector setup [--origin O] [--segment S ...] [--lenient]
    epc-connector update [--origin O] [--segment S ...] [--date-from D]
                         [--date-to D] [--lenient]
    epc-connector status
    epc-connector coverage [--origin O] [--element E ...]

``--origin`` is ``england-and-wales`` (``england`` and ``wales`` mean the
same), ``scotland`` or ``gb`` (both, the default). ``setup`` downloads the
selected datasets into ``--data-dir`` (default ``data``) and ``update``
refreshes them; both then bring the GB file up to date. The England and
Wales API token is read from ``EPB_API_TOKEN`` or ``.env``.

Each download, update and GB step runs in a fresh process, so its memory is
returned to the operating system when it ends.
"""

import argparse
import logging
import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from epc_connector import cleaning
from epc_connector.datasets import Origin, Segment, parse_origins
from epc_connector.errors import EpcConnectorError
from epc_connector.pipeline import coverage, plan, process, status
from epc_connector.sources.england_and_wales import authorised_client

MIN_FREE_STAGING_BYTES: Final = 10 * 1024**3
"""Free staging space a Scottish domestic run needs (about 8 GB, plus room)."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line interface; return the exit status."""
    args = build_parser().parse_args(argv)
    process.configure_logging()
    try:
        return args.run(args)
    except EpcConnectorError as error:
        logging.error("%s", error)
        return 1


def build_parser() -> argparse.ArgumentParser:
    """Return the parser for every subcommand."""
    data = argparse.ArgumentParser(add_help=False)
    data.add_argument("--data-dir", type=Path, default=Path("data"))
    origin = argparse.ArgumentParser(add_help=False)
    origin.add_argument(
        "--origin",
        help="england-and-wales (or england, wales), scotland or gb (default)",
    )
    writing = argparse.ArgumentParser(add_help=False, parents=[data, origin])
    writing.add_argument(
        "--segment",
        nargs="+",
        type=Segment,
        choices=list(Segment),
        metavar="SEGMENT",
        help="domestic, non-domestic or display (default: all, smallest first)",
    )
    writing.add_argument("--staging-dir", type=Path, default=Path(".staging"))
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
            "Download the selected datasets (all by default), then bring the "
            "GB file up to date."
        ),
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
        parents=[data],
        help="list which datasets and GB files are set up",
        description="List which datasets and GB files exist in --data-dir.",
    )
    status_command.set_defaults(run=_status)

    coverage_command = commands.add_parser(
        "coverage",
        parents=[data, origin],
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
    return parser


def _setup(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    work = plan.plan_setup(
        parse_origins(args.origin),
        args.segment,
        workspace=workspace,
        strict=not args.lenient,
    )
    return _run(work, workspace)


def _update(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    work = plan.plan_update(
        parse_origins(args.origin),
        args.segment,
        date_from=args.date_from,
        date_to=args.date_to,
        workspace=workspace,
        strict=not args.lenient,
    )
    return _run(work, workspace)


def _status(args: argparse.Namespace) -> int:
    print("\n".join(status.status_lines(args.data_dir)))
    return 0


def _coverage(args: argparse.Namespace) -> int:
    elements = args.element or list(cleaning.Element)
    lines = coverage.coverage_lines(args.data_dir, parse_origins(args.origin), elements)
    print("\n".join(lines))
    return 0


def _run(work: plan.Plan, workspace: plan.Workspace) -> int:
    """Check prerequisites, then run every step of a plan in order."""
    scottish_domestic = any(
        dataset.origin is Origin.SCOTLAND and dataset.segment is Segment.DOMESTIC
        for dataset in work.datasets
    )
    if scottish_domestic and not _enough_staging_space(workspace.staging_dir):
        return 1
    if any(dataset.origin is Origin.ENGLAND_AND_WALES for dataset in work.datasets):
        try:  # fail now, not after the Scottish download, without a token
            authorised_client().close()
        except ValueError as error:
            logging.error("%s", error)
            return 1
    for step in work.steps:
        logging.info("Starting: %s", step.name)
        path = process.run_step(step.function, *step.args, **step.kwargs)
        logging.info("Done: %s -> %s", step.name, path)
    for segment in work.not_set_up:
        logging.warning(
            "GB %s is not set up yet; only the raw %s files were changed",
            segment,
            segment,
        )
    return 0


def _workspace(args: argparse.Namespace) -> plan.Workspace:
    return plan.Workspace(
        data_dir=args.data_dir,
        staging_dir=args.staging_dir,
        chunk_rows=args.chunk_rows,
        row_group_rows=args.row_group_rows,
    )


def _enough_staging_space(staging_dir: Path) -> bool:
    """Check for the free space a Scottish domestic run needs."""
    staging = Path(staging_dir).resolve()
    existing = next(p for p in [staging, *staging.parents] if p.exists())
    free = shutil.disk_usage(existing).free
    if free >= MIN_FREE_STAGING_BYTES:
        return True
    logging.error(
        "Only %.1f GB free under %s; Scottish domestic needs about 10 GB",
        free / 1024**3,
        staging_dir,
    )
    return False
