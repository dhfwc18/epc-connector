# epc-connector

Download, update, clean and merge UK domestic Energy Performance Certificate (EPC) data from England and Wales and from
Scotland into one Great Britain Parquet file.

## Command line

```bash
uv sync
uv run epc-connector setup                                       # download every dataset, then build the GB file
uv run epc-connector setup --origin scotland --segment domestic  # one dataset; replaces its rows in the GB file
uv run epc-connector update --date-from 2026-09-01               # refresh downloaded datasets, then the GB file
uv run epc-connector status                                      # which datasets and GB files are set up
uv run epc-connector coverage --element walls                    # vocabulary coverage of the raw descriptions
```

`--origin` is `england-and-wales` (`england` and `wales` mean the same), `scotland` or `gb` (both, the default);
`--segment` is any of `domestic`, `non-domestic` and `display` (default: all). Only domestic data has a GB file
(`data/domestic_epc_certificate_gb.parquet`); non-domestic and display certificates are downloaded but not merged yet.
The England and Wales API token is read from `EPB_API_TOKEN` or `.env`.

`uv run epc-connector <command> --help` lists each command's options; `python -m epc_connector` works too. Each step
runs in its own process, so memory is released between steps.

## Python

```python
from epc_connector import build_gb, download

download("scotland", "domestic")
build_gb()
```
