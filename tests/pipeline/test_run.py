"""
Tests for ``epc_connector.pipeline.run``.
"""

import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from epc_connector.datasets import get_dataset
from epc_connector.pipeline import plan, run
from epc_connector.storage import StorageError


def path_of_process(name: str) -> Path:
    """Step stand-in: return a path naming the process that ran it."""
    return Path(name) / str(os.getpid())


def one_step_plan(*datasets: str) -> plan.Plan:
    return plan.Plan(
        datasets=[get_dataset(*name.split("/")) for name in datasets],
        steps=[plan.Step("record", path_of_process, ("out",))],
        not_set_up=[],
    )


@pytest.fixture
def workspace(tmp_path: Path) -> plan.Workspace:
    return plan.Workspace.resolve(tmp_path)


@pytest.fixture
def enough_space(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "shutil.disk_usage", lambda path: SimpleNamespace(free=100 * 1024**3)
    )


class TestRunPlan:
    def test_steps_run_in_this_process_by_default(
        self, workspace: plan.Workspace, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            run.run_plan(one_step_plan(), workspace)

        assert f"out/{os.getpid()}" in caplog.text

    def test_isolated_steps_run_in_another_process(
        self, workspace: plan.Workspace, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            run.run_plan(one_step_plan(), workspace, isolated=True)

        assert "Step 1 done" in caplog.text
        assert f"out/{os.getpid()}" not in caplog.text

    def test_empty_plan_does_nothing(
        self, workspace: plan.Workspace, caplog: pytest.LogCaptureFixture
    ) -> None:
        work = plan.Plan(datasets=[], steps=[], not_set_up=[])

        with caplog.at_level(logging.INFO):
            run.run_plan(work, workspace)

        assert "Nothing to do" in caplog.text

    def test_scottish_domestic_without_space_raises_before_any_step(
        self, workspace: plan.Workspace, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "shutil.disk_usage", lambda path: SimpleNamespace(free=1024**3)
        )

        with pytest.raises(StorageError, match="about 10 GB"):
            run.run_plan(one_step_plan("scotland/domestic"), workspace)

    @pytest.mark.usefixtures("enough_space")
    def test_england_and_wales_checks_the_token_first(
        self, workspace: plan.Workspace, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        checked: list[bool] = []
        client = SimpleNamespace(close=lambda: checked.append(True))
        monkeypatch.setattr(run, "authorised_client", lambda: client)

        run.run_plan(one_step_plan("england-and-wales/domestic"), workspace)

        assert checked == [True]
