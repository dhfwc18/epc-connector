"""
Tests for the ``epc_connector`` package surface.
"""

import subprocess
import sys

import pytest

import epc_connector
from epc_connector import api


class TestPackage:
    def test_every_public_name_is_importable(self) -> None:
        for name in epc_connector.__all__:
            assert hasattr(epc_connector, name), name

    def test_functions_are_the_api_ones(self) -> None:
        assert (
            epc_connector.load_domestic_certificates is api.load_domestic_certificates
        )
        assert epc_connector.update is api.update
        assert epc_connector.status is api.status

    @pytest.mark.parametrize(
        "name", ["download", "build_gb", "replace_origin_in_gb", "clean_domestic_epc"]
    )
    def test_setup_steps_are_not_public(self, name: str) -> None:
        assert not hasattr(epc_connector, name)

    def test_import_leaves_the_command_line_unloaded(self) -> None:
        # A fresh interpreter: this test session has imported the CLI already.
        code = (
            "import sys, epc_connector; "
            "print('epc_connector.pipeline.cli' in sys.modules)"
        )

        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )

        assert result.stdout.strip() == "False"
