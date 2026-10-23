"""
Tests for ``epc_connector.pipeline.plan``.
"""

from pathlib import Path

import polars as pl
import pytest

from epc_connector import sources
from epc_connector.canonical import gb
from epc_connector.datasets import (
    RELEASE_METADATA_KEY,
    DatasetError,
    Origin,
    Segment,
    get_dataset,
)
from epc_connector.pipeline import plan


def touch_datasets(data_dir: Path, *names: str) -> None:
    """Create empty stand-ins for downloads, e.g. ``"scotland/domestic"``."""
    data_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        get_dataset(*name.split("/")).path(data_dir).touch()


def step_summary(work: plan.Plan) -> list[tuple[object, tuple]]:
    return [(step.function, step.args) for step in work.steps]


class TestPlanSetup:
    def test_every_dataset_then_build_gb_by_default(self) -> None:
        work = plan.plan_setup(list(Origin))

        assert step_summary(work) == [
            (sources.download, (Origin.SCOTLAND, Segment.NON_DOMESTIC)),
            (sources.download, (Origin.SCOTLAND, Segment.DOMESTIC)),
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.NON_DOMESTIC)),
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.DISPLAY)),
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.DOMESTIC)),
            (gb.build_gb, ()),
        ]
        assert work.not_set_up == [Segment.NON_DOMESTIC, Segment.DISPLAY]

    def test_one_origin_replaces_only_its_rows_in_gb(self) -> None:
        work = plan.plan_setup([Origin.SCOTLAND], [Segment.DOMESTIC])

        assert step_summary(work) == [
            (sources.download, (Origin.SCOTLAND, Segment.DOMESTIC)),
            (gb.replace_origin_in_gb, (Origin.SCOTLAND,)),
        ]
        assert work.not_set_up == []

    def test_segment_without_gb_file_has_no_gb_step(self) -> None:
        work = plan.plan_setup(list(Origin), [Segment.DISPLAY])

        assert step_summary(work) == [
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.DISPLAY)),
        ]
        assert work.not_set_up == [Segment.DISPLAY]

    def test_workspace_and_strictness_reach_the_steps(self, tmp_path: Path) -> None:
        workspace = plan.Workspace.resolve(tmp_path / "elsewhere", chunk_rows=5)

        work = plan.plan_setup(
            [Origin.SCOTLAND], [Segment.DOMESTIC], workspace=workspace, strict=False
        )

        download, gb_step = work.steps
        assert download.kwargs == workspace.options()
        assert gb_step.kwargs == workspace.options() | {"strict": False}

    def test_downloaded_datasets_are_skipped(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "scotland/domestic")
        (tmp_path / gb.GB_PARQUET_NAME).touch()
        workspace = plan.Workspace.resolve(tmp_path)

        work = plan.plan_setup(list(Origin), [Segment.DOMESTIC], workspace=workspace)

        assert [d.name for d in work.skipped] == ["scotland/domestic"]
        assert step_summary(work) == [
            (sources.download, (Origin.ENGLAND_AND_WALES, Segment.DOMESTIC)),
            (gb.replace_origin_in_gb, (Origin.ENGLAND_AND_WALES,)),
        ]

    def test_everything_downloaded_plans_no_steps(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "scotland/domestic", "england-and-wales/domestic")
        (tmp_path / gb.GB_PARQUET_NAME).touch()
        workspace = plan.Workspace.resolve(tmp_path)

        work = plan.plan_setup(list(Origin), [Segment.DOMESTIC], workspace=workspace)

        assert work.steps == []
        assert len(work.skipped) == 2

    def test_missing_gb_file_is_built_from_downloaded_files(
        self, tmp_path: Path
    ) -> None:
        touch_datasets(tmp_path, "scotland/domestic")
        workspace = plan.Workspace.resolve(tmp_path)

        work = plan.plan_setup(
            [Origin.SCOTLAND], [Segment.DOMESTIC], workspace=workspace
        )

        assert step_summary(work) == [(gb.build_gb, ())]

    def test_force_downloads_again(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "scotland/domestic")
        workspace = plan.Workspace.resolve(tmp_path)

        work = plan.plan_setup(
            [Origin.SCOTLAND], [Segment.DOMESTIC], workspace=workspace, force=True
        )

        assert work.skipped == []
        assert step_summary(work)[0] == (
            sources.download,
            (Origin.SCOTLAND, Segment.DOMESTIC),
        )

    def test_default_workspace_uses_the_default_directories(
        self, default_data_dir: Path
    ) -> None:
        work = plan.plan_setup([Origin.SCOTLAND], [Segment.DOMESTIC])

        assert work.steps[0].kwargs["data_dir"] == default_data_dir
        assert work.steps[0].kwargs["staging_dir"] == default_data_dir / ".staging"

    def test_unpublished_pair_raises(self) -> None:
        with pytest.raises(DatasetError, match="display"):
            plan.plan_setup([Origin.SCOTLAND], [Segment.DISPLAY])


class TestPlanUpdate:
    def test_window_reaches_every_update_step(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "england-and-wales/domestic")

        work = plan.plan_update(
            [Origin.ENGLAND_AND_WALES],
            [Segment.DOMESTIC],
            date_from="2026-09-01",
            workspace=plan.Workspace.resolve(tmp_path),
        )

        update, gb_step = work.steps
        assert (update.function, update.args) == (
            sources.update,
            (Origin.ENGLAND_AND_WALES, Segment.DOMESTIC),
        )
        assert update.kwargs["date_from"] == "2026-09-01"
        assert update.kwargs["date_to"] is None
        assert "date_from" not in gb_step.kwargs
        assert gb_step.args == (Origin.ENGLAND_AND_WALES,)

    def test_datasets_never_downloaded_are_left_out(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "scotland/domestic")

        work = plan.plan_update(
            list(Origin), [Segment.DOMESTIC], workspace=plan.Workspace.resolve(tmp_path)
        )

        assert [d.name for d in work.datasets] == ["scotland/domestic"]
        assert [d.name for d in work.not_downloaded] == ["england-and-wales/domestic"]


def write_gb_metadata(data_dir: Path, metadata: dict[str, str]) -> None:
    """Write a stand-in GB file that carries only provenance metadata."""
    pl.DataFrame({"a": [1]}).write_parquet(
        data_dir / gb.GB_PARQUET_NAME, metadata=metadata
    )


def write_raw_metadata(data_dir: Path, name: str, metadata: dict[str, str]) -> None:
    """Write a stand-in raw file that carries only provenance metadata."""
    data_dir.mkdir(parents=True, exist_ok=True)
    path = get_dataset(*name.split("/")).path(data_dir)
    pl.DataFrame({"a": [1]}).write_parquet(path, metadata=metadata)


class TestPlanDomestic:
    def test_missing_origin_is_downloaded_and_added_to_gb(self, tmp_path: Path) -> None:
        work = plan.plan_domestic(
            Origin.SCOTLAND, workspace=plan.Workspace.resolve(tmp_path)
        )

        assert step_summary(work) == [
            (sources.download, (Origin.SCOTLAND, Segment.DOMESTIC)),
            (gb.replace_origin_in_gb, (Origin.SCOTLAND,)),
        ]

    def test_origin_in_gb_at_its_raw_version_needs_nothing(
        self, tmp_path: Path
    ) -> None:
        write_raw_metadata(
            tmp_path, "scotland/domestic", {RELEASE_METADATA_KEY: "2026q2"}
        )
        write_gb_metadata(tmp_path, {RELEASE_METADATA_KEY: "2026q2"})

        work = plan.plan_domestic(
            Origin.SCOTLAND, workspace=plan.Workspace.resolve(tmp_path)
        )

        assert work.steps == []

    def test_gb_rows_from_another_raw_version_are_rebuilt(self, tmp_path: Path) -> None:
        write_raw_metadata(
            tmp_path, "scotland/domestic", {RELEASE_METADATA_KEY: "2026q3"}
        )
        write_gb_metadata(tmp_path, {RELEASE_METADATA_KEY: "2026q2"})

        work = plan.plan_domestic(
            Origin.SCOTLAND, workspace=plan.Workspace.resolve(tmp_path)
        )

        assert step_summary(work) == [(gb.replace_origin_in_gb, (Origin.SCOTLAND,))]

    def test_other_origins_and_segments_are_not_touched(self, tmp_path: Path) -> None:
        work = plan.plan_domestic(
            Origin.ENGLAND_AND_WALES, workspace=plan.Workspace.resolve(tmp_path)
        )

        assert [d.name for d in work.datasets] == ["england-and-wales/domestic"]


class TestApiToken:
    def test_token_reaches_england_and_wales_steps_only(self, tmp_path: Path) -> None:
        work = plan.plan_setup(
            list(Origin),
            [Segment.DOMESTIC],
            workspace=plan.Workspace.resolve(tmp_path),
            api_token="secret-token",
        )

        tokens = {
            step.args[0]: step.kwargs.get("api_token")
            for step in work.steps
            if step.function is sources.download
        }
        assert tokens == {
            Origin.SCOTLAND: None,
            Origin.ENGLAND_AND_WALES: "secret-token",
        }

    def test_step_repr_hides_the_token(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "england-and-wales/domestic")
        work = plan.plan_update(
            [Origin.ENGLAND_AND_WALES],
            [Segment.DOMESTIC],
            workspace=plan.Workspace.resolve(tmp_path),
            api_token="secret-token",
        )

        assert "secret-token" in str(work.steps[0].kwargs)
        assert "secret-token" not in repr(work)


class TestDownloads:
    def test_setup_lists_the_datasets_it_downloads(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "scotland/domestic")

        work = plan.plan_setup(
            list(Origin), [Segment.DOMESTIC], workspace=plan.Workspace.resolve(tmp_path)
        )

        assert [d.name for d in work.downloads] == ["england-and-wales/domestic"]

    def test_update_downloads_nothing_in_full(self, tmp_path: Path) -> None:
        touch_datasets(tmp_path, "scotland/domestic")

        work = plan.plan_update(
            [Origin.SCOTLAND], workspace=plan.Workspace.resolve(tmp_path)
        )

        assert work.downloads == []

    def test_domestic_lists_a_missing_raw_file(self, tmp_path: Path) -> None:
        work = plan.plan_domestic(
            Origin.SCOTLAND, workspace=plan.Workspace.resolve(tmp_path)
        )

        assert [d.name for d in work.downloads] == ["scotland/domestic"]
