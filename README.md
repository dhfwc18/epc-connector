# epc-connector

Download, update, clean and merge UK domestic Energy Performance Certificate (EPC) data from England and Wales and from
Scotland into one Great Britain Parquet file.

## England and Wales API token

England and Wales data needs an API token from
[Get energy performance of buildings data](https://get-energy-performance-data.communities.gov.uk/) (sign in with GOV.UK
One Login). Scottish data needs none.

**Command line.** The first command that needs the token asks for it (input hidden), checks it with the API and saves it
to your user configuration directory (`~/.config/epc-connector/token` on Linux), readable by you only, as git's
credential store does. Later runs use the saved token. `epc-connector login` replaces it and `epc-connector logout`
deletes it. In CI or scripts, where nothing can be typed, set `EPB_API_TOKEN` instead; it takes precedence over the
saved token.

**Python.** Pass the token explicitly:

```python
import os

import epc_connector as epc

# If you use a .env file load it first (python-dotenv):
# from dotenv import load_dotenv
# load_dotenv()
token = os.environ["EPB_API_TOKEN"]
homes = epc.load_domestic_certificates(api_token=token)
epc.update(api_token=token)
```

Without `api_token`, the functions use `EPB_API_TOKEN`, then the token saved by the command line.

## Command line

```bash
uv sync
uv run epc-connector setup                                       # download what is missing, then build the GB file
uv run epc-connector setup --origin scotland --segment domestic  # one dataset; replaces its rows in the GB file
uv run epc-connector update --date-from 2026-09-01               # refresh downloaded datasets, then the GB file
uv run epc-connector status                                      # which datasets and GB files are set up
uv run epc-connector coverage --element walls                    # vocabulary coverage of the raw descriptions
```

`--origin` is `england-and-wales` (`england` and `wales` mean the same), `scotland` or `gb` (both, the default);
`--segment` is any of `domestic`, `non-domestic` and `display` (default: all). Only domestic data has a GB file
(`domestic_epc_certificate_gb.parquet`); non-domestic and display certificates are downloaded but not merged yet. See
[England and Wales API token](#england-and-wales-api-token) for the token.

Every command takes `--debug` to show every log message (HTTP requests, archive members, temporary files) and
`--log-file PATH` to append every message, debug ones included, to a file whatever the console shows.

`uv run epc-connector <command> --help` lists each command's options; `python -m epc_connector` works too. Each step
runs in its own process, so memory is released between steps.

## Where the data lives

Data goes to `.epc_data/` in the directory you run from, so each project keeps its own copy next to its `.venv`. When
the package creates the folder it puts a `.gitignore` (`*`) and a `CACHEDIR.TAG` in it, so git and backup tools skip it
without any change to your project. Running `setup` again only downloads what is missing (`--force` downloads everything
again); `update` refreshes what is there.

To share one copy between projects, set `EPC_CONNECTOR_DATA_DIR` to a directory, for example in your shell profile.
`--data-dir` (or `data_dir=` in Python) overrides both. Downloads are staged in `.staging/` inside the data directory,
which is removed when each step ends; `--staging-dir` puts it elsewhere.

## Python

```python
import polars as pl
import epc_connector as epc

# The first call downloads and builds what it needs: Scottish domestic data only.
homes = epc.load_domestic_certificates("scotland", columns=["uprn", "current_energy_rating"])

# GB data (both origins), lazily: nothing is read until it is collected.
ratings = (
    epc.load_domestic_certificates(lazy=True)
    .group_by("country", "current_energy_rating")
    .len()
    .collect(engine="streaming")
)

epc.update()  # same as epc-connector update
epc.status()  # same as epc-connector status, as data
```

`load_domestic_certificates` returns a Polars `DataFrame`, or a `LazyFrame` with `lazy=True`. Loading every column of
every certificate needs far more memory than the file on disk, so pass `columns` or filter a lazy frame before
collecting. Only what the call needs is set up: the raw domestic file of each origin asked for and its rows in the GB
file; non-domestic and display data are never downloaded by it. It warns when an origin's data is more than three months
old, and when GB data holds one origin only (for example without an
[England and Wales API token](#england-and-wales-api-token)). Every error the package raises on purpose is an
`EpcConnectorError`.

Only the names exported from `epc_connector` (`load_domestic_certificates`, `update`, `status`, `DatasetStatus`,
`Origin`, `Segment` and the errors) are the public API. Submodules such as `epc_connector.sources` or
`epc_connector.canonical` are internal and may change in any release.
