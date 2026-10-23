"""
Tests for ``epc_connector._cli.coverage``.
"""

from pathlib import Path

from epc_connector._cleaning import Element
from epc_connector._cli import coverage
from epc_connector._datasets import Origin
from tests.fakes import write_raw_domestic


class TestCoverageLines:
    def test_column_is_counted_and_its_outputs_listed(self, tmp_path: Path) -> None:
        rows = [{"MAIN_FUEL": "mains gas (not community)"}, {"MAIN_FUEL": ""}]
        write_raw_domestic(tmp_path, Origin.SCOTLAND, rows, {})

        lines = coverage.coverage_lines(
            tmp_path, [Origin.SCOTLAND], [Element.MAIN_FUEL]
        )

        output = "\n".join(lines)
        assert "## scotland / main-fuel (MAIN_FUEL)" in output
        assert "rows 2, distinct 2" in output
        assert "main_fuel_type" in output
