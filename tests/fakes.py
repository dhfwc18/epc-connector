"""
Builders for synthetic test inputs and an in-memory HTTP server.

Everything is a few kilobytes; no test touches the network or real data.
"""

import json
import zipfile
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import httpx
import polars as pl
import py7zr

from epc_connector.datasets import Origin, Segment, get_dataset
from epc_connector.validation import load_schema

UTF8_BOM = "\ufeff"
LABEL_ROW = "Label A,Total floor area (m\ufffd),Label C"
SOURCE_URL = (
    "https://api.data.gov.scot/resource/00000000-0000-0000-0000-000000000000/download"
)
RELEASE_TITLE = "Domestic Energy Performance Certificates - Dataset to Q2 2026"
PRESIGNED_URL = "https://bucket.s3.amazonaws.com/archive.7z?X-Amz-Signature=abc"
CERTIFICATE = "/api/certificate"


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
    source_url
        URL that redirects to the file; ``<source_url>/info`` serves file info.
    token
        If set, ``source_url`` and its ``/info`` require this bearer token.
    last_updated
        Timestamp returned by ``<source_url>/info``.
    lodged
        ``(certificate number, registration date)`` pairs served by any
        ``.../search`` endpoint, filtered by date and paginated like the API.
    delta_events
        Events served by ``/api/deltas``.
    documents
        Certificate documents served by ``/api/certificate``, by number.

    The pre-signed URL rejects any request carrying an ``Authorization``
    header, as S3 does.
    """

    def __init__(
        self,
        payload: bytes,
        *,
        supports_ranges: bool = True,
        drop_after: int | None = None,
        title: str = RELEASE_TITLE,
        source_url: str = SOURCE_URL,
        token: str | None = None,
        last_updated: str = "2026-10-01T00:31:19.000+00:00",
        lodged: list[tuple[str, str]] | None = None,
        delta_events: list[dict[str, str]] | None = None,
        documents: dict[str, dict[str, object]] | None = None,
    ) -> None:
        self.documents = documents or {}
        self.lodged = lodged or []
        self.delta_events = delta_events or []
        self.bytes_served = 0
        self.payload = payload
        self.title = title
        self.source_url = source_url
        self.token = token
        self.last_updated = last_updated
        self.supports_ranges = supports_ranges
        self.drop_after = drop_after
        self.calls: Counter[str] = Counter()
        self.ranges: list[str | None] = []

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self))

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

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
        path = request.url.path
        api_call = path.endswith("/search") or path in ("/api/deltas", CERTIFICATE)
        needs_token = self.token and (
            api_call or url in (self.source_url, f"{self.source_url}/info")
        )
        if (
            needs_token
            and request.headers.get("authorization") != f"Bearer {self.token}"
        ):
            self.calls["unauthorised"] += 1
            return httpx.Response(401, json={"error": "Unauthorised"})
        if path.endswith("/search"):
            return self._search(request)
        if path == "/api/deltas":
            if not self.delta_events:
                return httpx.Response(404, json={"error": "No audit logs"})
            return httpx.Response(200, json={"data": self.delta_events})
        if path == CERTIFICATE:
            self.calls["certificate"] += 1
            document = self.documents.get(request.url.params["certificate_number"])
            if document is None:
                return httpx.Response(404, json={"error": "Certificate not found"})
            return httpx.Response(200, json={"data": document})
        if url == f"{self.source_url}/info":
            data = {"fileSize": len(self.payload), "lastUpdated": self.last_updated}
            return httpx.Response(200, json={"data": data})
        if url == self.source_url:
            self.calls["redirect"] += 1
            return httpx.Response(302, headers={"location": PRESIGNED_URL})
        if url != PRESIGNED_URL:
            return httpx.Response(404)
        if "authorization" in request.headers:
            self.calls["file_with_authorization"] += 1
            return httpx.Response(400, text="Only one auth mechanism allowed")
        self.calls["file"] += 1
        range_header = request.headers.get("range")
        self.ranges.append(range_header)
        size = len(self.payload)
        start, status = 0, 200
        headers = {"content-length": str(size)}
        end = size - 1
        if range_header and self.supports_ranges:
            first, _, last = range_header.removeprefix("bytes=").partition("-")
            start, end = int(first), min(int(last or end), end)
            status = 206
            headers = {
                "content-length": str(end - start + 1),
                "content-range": f"bytes {start}-{end}/{size}",
            }
        body = self.payload[start : end + 1]
        self.bytes_served += len(body)
        if self.drop_after is not None:
            cut, self.drop_after = self.drop_after, None
            return httpx.Response(status, headers=headers, content=_drop(body, cut))
        return httpx.Response(status, headers=headers, content=body)

    def _search(self, request: httpx.Request) -> httpx.Response:
        params = request.url.params
        hits = [
            {"certificateNumber": number, "registrationDate": date}
            for number, date in self.lodged
            if params["date_start"] <= date <= params["date_end"]
        ]
        if not hits:
            return httpx.Response(404, json={"error": "No certificates"})
        size, page = int(params["page_size"]), int(params["current_page"])
        pages = -(-len(hits) // size)
        self.calls["search_page"] += 1
        pagination = {
            "totalRecords": len(hits),
            "currentPage": page,
            "totalPages": pages,
            "nextPage": page + 1 if page < pages else None,
            "pageSize": size,
        }
        data = hits[(page - 1) * size : page * size]
        return httpx.Response(200, json={"data": data, "pagination": pagination})


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
    """Return a Scottish-style CSV of (report number, CREATED_AT, value)."""
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


def write_vocabularies(directory: Path) -> Path:
    """
    Write small walls and main-heating vocabularies for engine tests.

    Walls: construction, insulation (assumed or not) and a U-value read from
    ``u # w/m2k``. Main heating: systems counted by ``system``, with a second
    fuel going to ``other_fuel``.
    """
    directory.mkdir(parents=True, exist_ok=True)
    walls = {
        "element": "walls",
        "prefix": "walls",
        "fields": {
            "construction": {
                "type": "string",
                "values": {
                    "cavity wall": ["cavity wall", "waliau ceudod"],
                    "solid brick": ["solid brick", "solid brick, filled cavity"],
                },
            },
            "insulation": {
                "type": "string",
                "values": {"none": ["no insulation (assumed)"]},
            },
            "insulation_assumed": {
                "type": "boolean",
                "values": {"true": ["no insulation (assumed)"]},
            },
            "u_value": {"type": "number", "number_from": ["u # w/m2k"]},
        },
        "ignore": {"no detail": ["as built"]},
        "discard": {"placeholder text": ["sap05:walls"]},
    }
    heating = {
        "element": "main-heating",
        "prefix": "main_heating",
        "count_systems_by": "system",
        "fields": {
            "system": {
                "type": "string",
                "values": {"boiler": ["boiler"], "unspecified": [""]},
            },
            "fuel": {
                "type": "string",
                "second_value_field": "other_fuel",
                "values": {"mains gas": ["mains gas"], "oil": ["oil"]},
            },
            "other_fuel": {"type": "string"},
        },
    }
    for document in (walls, heating):
        path = directory / f"{document['element']}.vocabulary.json"
        path.write_text(json.dumps(document))
    return directory


def write_raw_domestic(
    data_dir: Path,
    origin: Origin | str,
    rows: list[dict[str, str]],
    metadata: dict[str, str],
) -> Path:
    """
    Write a raw domestic file with every source schema column, in schema order.

    Columns not given in a row are empty strings, as in the published files.
    """
    dataset = get_dataset(origin, Segment.DOMESTIC)
    columns = list(load_schema(dataset.source_schema())["properties"])
    data = {column: [row.get(column, "") for row in rows] for column in columns}
    path = dataset.path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data, schema=dict.fromkeys(columns, pl.String)).write_parquet(
        path, metadata=metadata
    )
    return path
