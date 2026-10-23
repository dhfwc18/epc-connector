"""
Tests for ``epc_connector._datasets``.
"""

import logging
from pathlib import Path

import pytest

from epc_connector._datasets import (
    DATASETS,
    LAST_UPDATED_METADATA_KEY,
    RELEASE_METADATA_KEY,
    DatasetError,
    Origin,
    Segment,
    get_dataset,
    parse_origins,
    select_datasets,
)


class TestDataset:
    def test_name_joins_origin_and_segment(self) -> None:
        assert get_dataset("scotland", "domestic").name == "scotland/domestic"

    def test_every_source_schema_exists(self) -> None:
        for dataset in DATASETS.values():
            assert dataset.source_schema().is_file(), dataset.name

    def test_path_is_inside_the_data_directory(self) -> None:
        dataset = get_dataset(Origin.SCOTLAND, Segment.DOMESTIC)

        assert dataset.path("elsewhere") == Path(
            "elsewhere/domestic_epc_certificate_scotland.parquet"
        )

    def test_provenance_key_depends_on_the_publisher(self) -> None:
        scotland = get_dataset(Origin.SCOTLAND, Segment.NON_DOMESTIC)
        england_and_wales = get_dataset(Origin.ENGLAND_AND_WALES, Segment.DISPLAY)

        assert scotland.provenance_key == RELEASE_METADATA_KEY
        assert england_and_wales.provenance_key == LAST_UPDATED_METADATA_KEY

    def test_file_names_are_unique(self) -> None:
        names = [dataset.parquet_name for dataset in DATASETS.values()]

        assert len(names) == len(set(names))


class TestGetDataset:
    def test_unpublished_pair_raises_dataset_error(self) -> None:
        with pytest.raises(DatasetError, match="scotland does not publish display"):
            get_dataset("scotland", "display")

    @pytest.mark.parametrize(
        ("origin", "segment", "message"),
        [
            ("france", "domestic", "unknown origin"),
            ("scotland", "shed", "unknown segment"),
        ],
    )
    def test_unknown_value_raises_dataset_error_listing_choices(
        self, origin: str, segment: str, message: str
    ) -> None:
        with pytest.raises(DatasetError, match=message):
            get_dataset(origin, segment)


class TestParseOrigins:
    @pytest.mark.parametrize("name", [None, "gb"])
    def test_no_name_or_gb_means_every_origin(self, name: str | None) -> None:
        assert parse_origins(name) == [Origin.ENGLAND_AND_WALES, Origin.SCOTLAND]

    @pytest.mark.parametrize("name", ["england", "wales"])
    def test_england_or_wales_means_both_with_a_warning(
        self, name: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            origins = parse_origins(name)

        assert origins == [Origin.ENGLAND_AND_WALES]
        assert "published as one dataset" in caplog.text

    def test_origin_name_is_used_as_is(self) -> None:
        assert parse_origins("scotland") == [Origin.SCOTLAND]

    def test_unknown_name_raises_dataset_error(self) -> None:
        with pytest.raises(DatasetError, match="choose from"):
            parse_origins("france")


class TestSelectDatasets:
    def test_every_segment_by_default_in_registry_order(self) -> None:
        selected = select_datasets([Origin.SCOTLAND])

        assert [dataset.segment for dataset in selected] == [
            Segment.NON_DOMESTIC,
            Segment.DOMESTIC,
        ]

    def test_unpublished_pairs_are_left_out(self) -> None:
        selected = select_datasets(Origin, [Segment.DISPLAY])

        assert [dataset.name for dataset in selected] == ["england-and-wales/display"]

    def test_empty_selection_raises_dataset_error(self) -> None:
        with pytest.raises(DatasetError, match="no display certificates are published"):
            select_datasets([Origin.SCOTLAND], [Segment.DISPLAY])
