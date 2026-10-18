"""
Tests for ``epc_connector.errors``.
"""

import pytest

from epc_connector.cleaning import PhraseConflictError, UnknownPhraseError
from epc_connector.datasets import DatasetError
from epc_connector.errors import EpcConnectorError
from epc_connector.mapping import MappingError
from epc_connector.storage import StorageError
from epc_connector.validation import UnsupportedSchemaError


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
        ],
    )
    def test_package_errors_share_the_base_and_keep_their_builtin(
        self, error: type[Exception], builtin: type[Exception]
    ) -> None:
        assert issubclass(error, EpcConnectorError)
        assert issubclass(error, builtin)
