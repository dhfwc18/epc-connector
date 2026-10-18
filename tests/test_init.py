"""
Tests for the ``epc_connector`` package surface.
"""

import subprocess
import sys

import epc_connector
from epc_connector import canonical, cleaning, sources


class TestPackage:
    def test_every_public_name_is_importable(self) -> None:
        for name in epc_connector.__all__:
            assert hasattr(epc_connector, name), name

    def test_functions_are_the_library_ones(self) -> None:
        assert epc_connector.download is sources.download
        assert epc_connector.update is sources.update
        assert epc_connector.build_gb is canonical.build_gb
        assert epc_connector.replace_origin_in_gb is canonical.replace_origin_in_gb
        assert epc_connector.clean_domestic_epc is cleaning.clean_domestic_epc

    def test_import_leaves_the_command_line_unloaded(self) -> None:
        # A fresh interpreter: this test session has imported pipeline already.
        code = (
            "import sys, epc_connector; print('epc_connector.pipeline' in sys.modules)"
        )

        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )

        assert result.stdout.strip() == "False"
