"""
Tests for ``epc_connector.sources`` (the ``download`` and ``update`` entry).
"""

from pathlib import Path
from typing import Any

import pytest

from epc_connector import sources
from epc_connector.datasets import DatasetError, Origin, Segment


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, tuple, dict]]:
    """Replace every origin's download and update with a recorder."""
    recorded: list[tuple[str, tuple, dict]] = []

    def recorder(name: str):
        def record(*args: Any, **kwargs: Any) -> Path:
            recorded.append((name, args, kwargs))
            return Path(name)

        return record

    for origin in Origin:
        monkeypatch.setitem(sources._DOWNLOADS, origin, recorder(f"{origin} download"))
        monkeypatch.setitem(sources._UPDATES, origin, recorder(f"{origin} update"))
    return recorded


class TestDownload:
    def test_origin_module_is_called_with_the_segment_and_options(
        self, calls: list
    ) -> None:
        path = sources.download("scotland", "domestic", data_dir="elsewhere")

        assert path == Path("scotland download")
        assert calls == [
            ("scotland download", (Segment.DOMESTIC,), {"data_dir": "elsewhere"})
        ]

    def test_unpublished_pair_raises_before_anything_runs(self, calls: list) -> None:
        with pytest.raises(DatasetError, match="does not publish display"):
            sources.download("scotland", "display")

        assert calls == []


class TestUpdate:
    def test_origin_module_is_called_with_the_segment_and_options(
        self, calls: list
    ) -> None:
        sources.update(Origin.ENGLAND_AND_WALES, "display", date_from="2026-09-01")

        assert calls == [
            (
                "england-and-wales update",
                (Segment.DISPLAY,),
                {"date_from": "2026-09-01"},
            )
        ]
