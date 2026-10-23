"""
Tests for ``epc_connector._errors``.
"""

import pytest

from epc_connector._canonical import GbNotSetUpError, MappingError
from epc_connector._cleaning import PhraseConflictError, UnknownPhraseError
from epc_connector._datasets import DatasetError
from epc_connector._errors import EpcConnectorError
from epc_connector._io import StorageError
from epc_connector._validation import UnsupportedSchemaError


class TestEpcConnectorError:
    @pytest.mark.parametrize(
        ("error", "builtin"),
        [
            (StorageError, RuntimeError),
            (MappingError, ValueError),
            (UnsupportedSchemaError, ValueError),
            (UnknownPhraseError, ValueError),
            (PhraseConflictError, ValueError),
            (DatasetError, ValueError),
            (GbNotSetUpError, NotImplementedError),
        ],
    )
    def test_package_errors_share_the_base_and_keep_their_builtin(
        self, error: type[Exception], builtin: type[Exception]
    ) -> None:
        assert issubclass(error, EpcConnectorError)
        assert issubclass(error, builtin)
