"""
Tests for ``epc_connector.cli.process``.
"""

import logging
import os
from pathlib import Path

import pytest

from epc_connector.cli import process
from epc_connector.cli.logs import LogOptions


def path_of_process(name: str) -> Path:
    """Step stand-in: return a path naming the process that ran it."""
    return Path(name) / str(os.getpid())


def logging_step(name: str) -> Path:
    """Step stand-in: log a debug line from the step's process."""
    logging.getLogger("epc_connector.step").debug("inside the step")
    return Path(name)


def failing_step() -> Path:
    raise ValueError("step failed")


class TestRunStep:
    def test_step_runs_in_another_process_and_returns_its_result(self) -> None:
        result = process.run_step(path_of_process, "out")

        assert result.parent == Path("out")
        assert result.name != str(os.getpid())

    def test_error_in_the_step_is_raised(self) -> None:
        with pytest.raises(ValueError, match="step failed"):
            process.run_step(failing_step)

    def test_step_logs_with_the_options_given(self, tmp_path: Path) -> None:
        log_file = tmp_path / "run.log"

        process.run_step(logging_step, "out", log_options=LogOptions(log_file=log_file))

        [line] = log_file.read_text().splitlines()
        assert "SpawnProcess" in line
        assert line.endswith("epc_connector.step: inside the step")
