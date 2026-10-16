"""
Builders for synthetic test inputs and an in-memory HTTP server.

Everything is a few kilobytes; no test touches the network or real data.
"""

import zipfile
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import httpx
import py7zr

UTF8_BOM = "\ufeff"
LABEL_ROW = "Label A,Total floor area (m\ufffd),Label C"
SOURCE_URL = (
    "https://api.data.gov.scot/resource/00000000-0000-0000-0000-000000000000/download"
)
RELEASE_TITLE = "Domestic Energy Performance Certificates - Dataset to Q2 2026"
PRESIGNED_URL = "https://bucket.s3.amazonaws.com/archive.7z?X-Amz-Signature=abc"


def scottish_csv(data_rows: list[str], header: str = "A,B,C") -> bytes:
    """Return a CSV shaped like the Scottish files: BOM, label row, CRLF."""
    lines = [header, LABEL_ROW, *data_rows]
    return (UTF8_BOM + "\r\n".join(lines) + "\r\n").encode("utf-8")


def yearly_members() -> dict[str, bytes]:
    """Return three yearly CSVs, stored out of time order, plus a PDF note."""
    return {
        "2017.csv": scottish_csv(['3,"multi\r\nline",z', "4,,"]),
        "2015.csv": scottish_csv(["1,10.5,x"]),
        "2016.csv": scottish_csv(["2,0,y"]),
        "EPC Data - Publication Note.pdf": b"%PDF-1.4 placeholder",
    }


def write_7z(path: Path, members: dict[str, bytes]) -> Path:
    with py7zr.SevenZipFile(path, mode="w") as archive:
        for name, data in members.items():
            archive.writestr(data, name)
    return path


def write_zip(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return path


class FakeServer:
    """Redirect ``SOURCE_URL`` to a presigned URL serving ``payload``.

    Parameters
    ----------
    payload
        File content to serve.
    supports_ranges
        Honour ``Range`` headers; if False, always send the whole file.
    drop_after
        Cut the first response after this many bytes, like a lost connection.
    title
        Dataset title returned by CKAN ``package_show`` (holds the release).
    """

    def __init__(
        self,
        payload: bytes,
        *,
        supports_ranges: bool = True,
        drop_after: int | None = None,
        title: str = RELEASE_TITLE,
    ) -> None:
        self.payload = payload
        self.title = title
        self.supports_ranges = supports_ranges
        self.drop_after = drop_after
        self.calls: Counter[str] = Counter()
        self.ranges: list[str | None] = []

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/action/resource_show" in url:
            self.calls["resource_show"] += 1
            result = {"package_id": "package-1"}
            return httpx.Response(200, json={"success": True, "result": result})
        if "/action/package_show" in url:
            self.calls["package_show"] += 1
            result = {"title": self.title}
            return httpx.Response(200, json={"success": True, "result": result})
        if url == SOURCE_URL:
            self.calls["redirect"] += 1
            return httpx.Response(302, headers={"location": PRESIGNED_URL})
        if url != PRESIGNED_URL:
            return httpx.Response(404)
        self.calls["file"] += 1
        range_header = request.headers.get("range")
        self.ranges.append(range_header)
        size = len(self.payload)
        start, status = 0, 200
        headers = {"content-length": str(size)}
        if range_header and self.supports_ranges:
            start = int(range_header.removeprefix("bytes=").rstrip("-"))
            status = 206
            headers = {
                "content-length": str(size - start),
                "content-range": f"bytes {start}-{size - 1}/{size}",
            }
        body = self.payload[start:]
        if self.drop_after is not None:
            cut, self.drop_after = self.drop_after, None
            return httpx.Response(status, headers=headers, content=_drop(body, cut))
        return httpx.Response(status, headers=headers, content=body)


def _drop(body: bytes, cut: int) -> Iterator[bytes]:
    yield body[:cut]
    raise httpx.ReadError("connection dropped")


def abc_schema() -> dict:
    """Return a source schema for the A,B,C test CSVs; A must be an integer."""
    missing = {"$ref": "#/$defs/missing"}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "$defs": {"missing": {"enum": ["", "Data removed"]}},
        "properties": {
            "A": {"type": "string", "anyOf": [missing, {"pattern": "^[0-9]+$"}]},
            "B": {"title": "Label B", "type": "string"},
            "C": {"type": "string", "anyOf": [missing, {"enum": ["x", "y", "z"]}]},
        },
        "required": ["A", "B", "C"],
        "additionalProperties": False,
    }


def created_csv(rows: list[tuple[str, str, str]]) -> bytes:
    """Return a Scottish-style CSV of (report number, CREATED_AT, value) rows."""
    return scottish_csv(
        [",".join(row) for row in rows],
        header="REPORT_REFERENCE_NUMBER,CREATED_AT,VALUE",
    )


def created_schema() -> dict:
    """Return a source schema for ``created_csv`` files."""
    missing = {"$ref": "#/$defs/missing"}
    created = {"pattern": r"^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}$"}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "$defs": {"missing": {"enum": [""]}},
        "properties": {
            "REPORT_REFERENCE_NUMBER": {"type": "string"},
            "CREATED_AT": {"type": "string", "anyOf": [missing, created]},
            "VALUE": {"type": "string"},
        },
        "required": ["REPORT_REFERENCE_NUMBER", "CREATED_AT", "VALUE"],
        "additionalProperties": False,
    }
