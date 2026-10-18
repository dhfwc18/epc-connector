# epc-connector

Download, update, clean and merge UK domestic Energy Performance Certificate (EPC) data from England and Wales and from
Scotland into one Great Britain Parquet file.

## Command line

```bash
uv sync
uv run epc-connector setup                           # everything below in one go: download both nations, merge
uv run epc-connector download scotland
uv run epc-connector download england-and-wales      # API token in EPB_API_TOKEN or .env
uv run epc-connector merge                           # data/domestic_epc_certificate_gb.parquet
uv run epc-connector update england-and-wales --date-from 2026-09-01
uv run epc-connector coverage --element walls        # vocabulary coverage of the raw descriptions
```

`uv run epc-connector <command> --help` lists each command's options; `python -m epc_connector` works too. Each step
runs in its own process, so memory is released between steps.

## Python

```python
from epc_connector import build_gb, download

download("scotland", "domestic")
build_gb()
```
