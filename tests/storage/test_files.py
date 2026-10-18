"""
Tests for ``epc_connector.storage.files``.
"""

import os
from pathlib import Path

import pytest

from epc_connector.storage import files


class TestStreamingFile:
    @pytest.fixture
    def advised(self, monkeypatch: pytest.MonkeyPatch) -> list[int]:
        """Record each page cache drop instead of only performing it."""
        calls: list[int] = []
        drop = os.POSIX_FADV_DONTNEED

        def record(descriptor: int, offset: int, length: int, advice: int) -> None:
            if advice == drop:
                calls.append(descriptor)

        monkeypatch.setattr(os, "posix_fadvise", record)
        return calls

    def test_cache_is_dropped_every_block_and_on_close(
        self, tmp_path: Path, advised: list[int], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(files, "PAGE_CACHE_DROP_BYTES", 10)
        path = tmp_path / "out.bin"

        with files.StreamingFile(path) as output:
            for _ in range(5):
                output.write(b"x" * 4)

        assert path.read_bytes() == b"x" * 20
        assert len(advised) == 2  # once 12 bytes are written, and on close

    def test_drop_page_cache_flushes_and_drops_a_file(
        self, tmp_path: Path, advised: list[int]
    ) -> None:
        path = tmp_path / "data.bin"
        path.write_bytes(b"abc")

        files.drop_page_cache(path)

        assert len(advised) == 1


class TestStagingDirectory:
    def test_run_directory_and_empty_staging_are_removed(self, tmp_path: Path) -> None:
        staging = tmp_path / ".staging"

        with files.staging_directory(staging, prefix="run-") as run_dir:
            (run_dir / "file.csv").write_text("a")
            assert run_dir.parent == staging

        assert not staging.exists()

    def test_staging_used_by_another_run_is_kept(self, tmp_path: Path) -> None:
        staging = tmp_path / ".staging"
        staging.mkdir()
        (staging / "other-run").mkdir()

        with files.staging_directory(staging, prefix="run-") as run_dir:
            pass

        assert not run_dir.exists()
        assert staging.is_dir()

    def test_run_directory_is_removed_after_an_error(self, tmp_path: Path) -> None:
        staging = tmp_path / ".staging"

        with pytest.raises(RuntimeError), files.staging_directory(staging, "run-"):
            raise RuntimeError("boom")

        assert not staging.exists()


class TestAtomicWrite:
    def test_file_is_replaced_when_the_block_succeeds(self, tmp_path: Path) -> None:
        target = tmp_path / "data" / "out.parquet"
        target.parent.mkdir()
        target.write_text("old")

        with files.atomic_write(target, tmp_path) as temporary:
            temporary.write_text("new")
            assert target.read_text() == "old"

        assert target.read_text() == "new"
        assert not temporary.exists()

    def test_error_leaves_the_existing_file_untouched(self, tmp_path: Path) -> None:
        target = tmp_path / "out.parquet"
        target.write_text("old")
        work_dir = tmp_path / "work"
        work_dir.mkdir()

        with (
            pytest.raises(RuntimeError, match="failed"),
            files.atomic_write(target, work_dir) as temporary,
        ):
            temporary.write_text("half")
            raise RuntimeError("failed")

        assert target.read_text() == "old"

    def test_missing_directory_of_the_target_is_created(self, tmp_path: Path) -> None:
        target = tmp_path / "new" / "dir" / "out.parquet"

        with files.atomic_write(target, tmp_path) as temporary:
            temporary.write_text("x")

        assert target.read_text() == "x"
