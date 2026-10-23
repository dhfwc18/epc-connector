"""
Tests for ``epc_connector.pipeline.run``.
"""

import logging
import os
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from epc_connector.credentials import ApiTokenError
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

    def test_given_runner_calls_every_step(self, workspace: plan.Workspace) -> None:
        called: list[tuple[object, tuple[object, ...]]] = []

        def runner(function: Callable[..., Path], /, *args: Any, **kwargs: Any) -> Path:
            called.append((function, args))
            return Path("ran")

        run.run_plan(one_step_plan(), workspace, run_step=runner)

        assert called == [(path_of_process, ("out",))]

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

    def test_england_and_wales_without_a_token_raises_before_any_step(
        self, workspace: plan.Workspace, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO), pytest.raises(ApiTokenError):
            run.run_plan(one_step_plan("england-and-wales/domestic"), workspace)

        assert "Step 1" not in caplog.text

    def test_england_and_wales_runs_with_a_token(
        self, workspace: plan.Workspace, caplog: pytest.LogCaptureFixture
    ) -> None:
        work = one_step_plan("england-and-wales/domestic")

        with caplog.at_level(logging.INFO):
            run.run_plan(work, workspace, api_token="token")

        assert "Step 1 of 1" in caplog.text
