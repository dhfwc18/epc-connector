"""
Command line interface: ``epc-connector`` (or ``python -m epc_connector``).

Subcommands::

    epc-connector setup [--domestic-only] [--lenient]
    epc-connector download scotland [--series domestic]
    epc-connector download england-and-wales [--series domestic]
    epc-connector update scotland [--date-from 2025-01-01] [--date-to ...]
    epc-connector update england-and-wales [--date-from 2026-09-01]
    epc-connector merge [--lenient]
    epc-connector coverage [--nation scotland] [--element walls]

Files are read from and written to ``--data-dir`` (default ``data``). The
England and Wales API token is read from ``EPB_API_TOKEN`` or ``.env``.

Memory: each download, update and merge step runs in a fresh process, so all
its memory is returned to the operating system when the step ends and steps
never add up.
"""

import argparse
import logging
import multiprocessing
import shutil
import sys
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Final

import polars as pl

from epc_connector import cleaning, merge_gb_domestic_epc
from epc_connector.datasets import Origin, Segment, get_dataset
from epc_connector.sources import england_and_wales, scotland
from epc_connector.sources.england_and_wales import authorised_client

SCOTLAND_SERIES: Final = ["non-domestic", "domestic"]
"""Scottish series, smallest first."""
ENGLAND_AND_WALES_SERIES: Final = ["non-domestic", "display", "domestic"]
"""England and Wales series, smallest first."""
MIN_FREE_STAGING_BYTES: Final = 10 * 1024**3
"""Free staging space a Scottish domestic run needs (about 8 GB, plus room)."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line interface; return the exit status."""
    args = build_parser().parse_args(argv)
    _configure_logging()
    return args.run(args)


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def _run_step(function: Callable[..., Path], /, *args: Any, **kwargs: Any) -> Path:
    """
    Run one pipeline step in a fresh process and return the path it wrote.

    Polars and Python keep freed memory reserved inside a process; a separate
    process hands all of it back to the operating system when the step ends.
    Errors in the step are raised here.
    """
    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_configure_logging,
    ) as pool:
        path = pool.submit(function, *args, **kwargs).result()
    logging.info("Done: %s", path)
    return path


def build_parser() -> argparse.ArgumentParser:
    """Return the parser for every subcommand."""
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-dir", type=Path, default=Path("data"))
    writing = argparse.ArgumentParser(add_help=False, parents=[common])
    writing.add_argument("--staging-dir", type=Path, default=Path(".staging"))
    writing.add_argument("--chunk-rows", type=int, default=10_000)
    writing.add_argument("--row-group-rows", type=int, default=50_000)

    parser = argparse.ArgumentParser(
        prog="epc-connector",
        description="Download, update, clean and merge UK domestic EPC data.",
    )
    commands = parser.add_subparsers(required=True, metavar="command")

    for name, run, summary in [
        ("download", _download, "download series into Parquet files"),
        ("update", _update, "refresh downloaded files"),
    ]:
        command = commands.add_parser(name, help=summary, description=summary)
        nations = command.add_subparsers(required=True, metavar="nation")
        for nation, series in [
            ("scotland", SCOTLAND_SERIES),
            ("england-and-wales", ENGLAND_AND_WALES_SERIES),
        ]:
            sub = nations.add_parser(nation, parents=[writing], help=f"{nation} series")
            sub.add_argument(
                "--series",
                nargs="+",
                default=series,
                choices=series,
                help="series to process, in order (default: all, smallest first)",
            )
            if name == "update":
                sub.add_argument("--date-from", help="first date to refresh")
                sub.add_argument("--date-to", help="last date to refresh")
            sub.set_defaults(run=run, nation=nation)

    setup = commands.add_parser(
        "setup",
        parents=[writing],
        help="download every series of both nations, then merge",
        description=(
            "Run the full pipeline: download the Scottish and the England and Wales "
            "series, then write data/domestic_epc_certificate_gb.parquet."
        ),
    )
    setup.add_argument(
        "--domestic-only",
        action="store_true",
        help="download only the domestic series, which the GB file needs",
    )
    setup.add_argument(
        "--lenient",
        action="store_true",
        help="mark descriptions the vocabularies do not cover instead of failing",
    )
    setup.set_defaults(run=_setup)

    merge = commands.add_parser(
        "merge",
        parents=[writing],
        help="write data/domestic_epc_certificate_gb.parquet",
        description="Clean both raw domestic files and merge them into one GB file.",
    )
    merge.add_argument(
        "--lenient",
        action="store_true",
        help="mark descriptions the vocabularies do not cover instead of failing",
    )
    merge.set_defaults(run=_merge)

    coverage = commands.add_parser(
        "coverage",
        parents=[common],
        help="report how the vocabularies cover the raw descriptions",
        description="Report vocabulary coverage of the raw domestic descriptions.",
    )
    coverage.add_argument(
        "--nation", nargs="+", default=["england-and-wales", "scotland"]
    )
    coverage.add_argument("--element", nargs="+", help="elements (default: all)")
    coverage.set_defaults(run=_coverage)
    return parser


def _download(args: argparse.Namespace) -> int:
    if args.nation == "scotland" and not _enough_staging_space(args):
        return 1
    download = (
        scotland.download if args.nation == "scotland" else england_and_wales.download
    )
    for series in args.series:
        _run_step(download, series, **_writing_options(args))
    return 0


def _update(args: argparse.Namespace) -> int:
    if args.nation == "scotland" and not _enough_staging_space(args):
        return 1
    update = scotland.update if args.nation == "scotland" else england_and_wales.update
    for series in args.series:
        _run_step(
            update,
            series,
            date_from=args.date_from,
            date_to=args.date_to,
            **_writing_options(args),
        )
    return 0


def _setup(args: argparse.Namespace) -> int:
    """Download both nations' series, then merge; check prerequisites first."""
    scottish_series = ["domestic"] if args.domestic_only else SCOTLAND_SERIES
    english_and_welsh_series = (
        ["domestic"] if args.domestic_only else ENGLAND_AND_WALES_SERIES
    )
    staging_check = argparse.Namespace(**vars(args), series=scottish_series)
    if not _enough_staging_space(staging_check):
        return 1
    try:  # fail now, not after the Scottish download, if the token is missing
        authorised_client().close()
    except ValueError as error:
        logging.error("%s", error)
        return 1
    for series in scottish_series:
        _run_step(scotland.download, series, **_writing_options(args))
    for series in english_and_welsh_series:
        _run_step(england_and_wales.download, series, **_writing_options(args))
    return _merge(args)


def _merge(args: argparse.Namespace) -> int:
    _run_step(merge_gb_domestic_epc, strict=not args.lenient, **_writing_options(args))
    return 0


def _coverage(args: argparse.Namespace) -> int:
    elements = [cleaning.Element(e) for e in args.element or list(cleaning.Element)]
    for nation in map(Origin, args.nation):
        path = get_dataset(nation, Segment.DOMESTIC).path(args.data_dir)
        for element in elements:
            column = cleaning.SOURCE_COLUMNS[nation][element]
            counts = (
                pl.scan_parquet(path, low_memory=True)
                .group_by(pl.col(column).alias("raw"))
                .agg(pl.len().alias("rows"))
                .collect(engine="streaming")
            )
            started = time.perf_counter()
            table = cleaning.clean_values(counts["raw"], element, nation, strict=False)
            elapsed = time.perf_counter() - started
            rows = counts.join(table, on="raw", how="left")
            total = int(rows["rows"].sum())
            discarded = f"{cleaning.load_vocabulary(element).prefix}_discarded"
            print(f"\n## {nation} / {element} ({column})")
            print(
                f"rows {total:,}, distinct {counts.height:,}, cleaned in {elapsed:.2f}s"
            )
            reasons = (
                rows.drop_nulls(discarded)
                .group_by(discarded)
                .agg(pl.col("rows").sum())
                .sort("rows", descending=True)
            )
            for reason, count in reasons.iter_rows():
                print(f"  {count:>11,}  discarded: {reason}")
            for name in table.columns[1:]:
                filled = int(rows.filter(pl.col(name).is_not_null())["rows"].sum())
                print(f"  {name:<46} {100 * filled / max(total, 1):6.2f}% filled")
    return 0


def _writing_options(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "data_dir": args.data_dir,
        "staging_dir": args.staging_dir,
        "chunk_rows": args.chunk_rows,
        "row_group_rows": args.row_group_rows,
    }


def _enough_staging_space(args: argparse.Namespace) -> bool:
    """Check for the 8 GB of free space a Scottish domestic run needs."""
    if "domestic" not in args.series:
        return True
    staging = Path(args.staging_dir).resolve()
    existing = next(p for p in [staging, *staging.parents] if p.exists())
    free = shutil.disk_usage(existing).free
    if free >= MIN_FREE_STAGING_BYTES:
        return True
    logging.error(
        "Only %.1f GB free under %s; Scottish domestic needs about 10 GB",
        free / 1024**3,
        args.staging_dir,
    )
    return False


if __name__ == "__main__":
    sys.exit(main())
