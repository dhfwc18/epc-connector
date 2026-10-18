"""
Tests for ``epc_connector.pipeline.plan``.
"""

from pathlib import Path

import pytest

from epc_connector import sources
from epc_connector.canonical import gb
from epc_connector.datasets import DatasetError, Origin, Segment
from epc_connector.pipeline import plan


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

    def test_workspace_and_strictness_reach_the_steps(self) -> None:
        workspace = plan.Workspace(data_dir=Path("elsewhere"), chunk_rows=5)

        work = plan.plan_setup(
            [Origin.SCOTLAND], [Segment.DOMESTIC], workspace=workspace, strict=False
        )

        download, gb_step = work.steps
        assert download.kwargs == workspace.options()
        assert gb_step.kwargs == workspace.options() | {"strict": False}

    def test_unpublished_pair_raises(self) -> None:
        with pytest.raises(DatasetError, match="display"):
            plan.plan_setup([Origin.SCOTLAND], [Segment.DISPLAY])


class TestPlanUpdate:
    def test_window_reaches_every_update_step(self) -> None:
        work = plan.plan_update(
            [Origin.ENGLAND_AND_WALES],
            [Segment.DOMESTIC],
            date_from="2026-09-01",
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
