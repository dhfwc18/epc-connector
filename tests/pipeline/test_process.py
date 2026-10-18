"""
Tests for ``epc_connector.pipeline.process``.
"""

import os
from pathlib import Path

import pytest

from epc_connector.pipeline import process


def path_of_process(name: str) -> Path:
    """Step stand-in: return a path naming the process that ran it."""
    return Path(name) / str(os.getpid())


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
