"""
Report how the vocabularies cover the descriptions in the raw domestic files.

Run after each data update: a description the vocabularies do not cover
stops ``build_gb`` in strict mode, and this report lists them all at once.
"""

import time
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from epc_connector import cleaning
from epc_connector.datasets import Origin, Segment, get_dataset


def coverage_lines(
    data_dir: Path | str,
    origins: Iterable[Origin],
    elements: Iterable[cleaning.Element],
) -> list[str]:
    """
    Return a coverage report for each origin and element.

    For each description column: rows, distinct values, cleaning time,
    discarded rows by reason and how often each output column is filled.
    Only distinct values are cleaned, one column at a time.
    """
    lines: list[str] = []
    elements = list(elements)
    for origin in origins:
        path = get_dataset(origin, Segment.DOMESTIC).path(data_dir)
        for element in elements:
            column = cleaning.SOURCE_COLUMNS[origin][element]
            counts = (
                pl.scan_parquet(path, low_memory=True)
                .group_by(pl.col(column).alias("raw"))
                .agg(pl.len().alias("rows"))
                .collect(engine="streaming")
            )
            started = time.perf_counter()
            table = cleaning.clean_values(counts["raw"], element, origin, strict=False)
            elapsed = time.perf_counter() - started
            rows = counts.join(table, on="raw", how="left")
            total = int(rows["rows"].sum())
            discarded = f"{cleaning.load_vocabulary(element).prefix}_discarded"
            lines.append(f"\n## {origin} / {element} ({column})")
            lines.append(
                f"rows {total:,}, distinct {counts.height:,}, "
                f"cleaned in {elapsed:.2f}s"
            )
            reasons = (
                rows.drop_nulls(discarded)
                .group_by(discarded)
                .agg(pl.col("rows").sum())
                .sort("rows", descending=True)
            )
            for reason, count in reasons.iter_rows():
                lines.append(f"  {count:>11,}  discarded: {reason}")
            for name in table.columns[1:]:
                filled = int(rows.filter(pl.col(name).is_not_null())["rows"].sum())
                lines.append(
                    f"  {name:<46} {100 * filled / max(total, 1):6.2f}% filled"
                )
    return lines
