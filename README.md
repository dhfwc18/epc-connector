# epc-connector

Download, update, clean and merge UK domestic Energy Performance Certificate (EPC) data from England and Wales and from
Scotland into one Great Britain Parquet file.

## England and Wales API token

England and Wales data needs an API token from
[Get energy performance of buildings data](https://get-energy-performance-data.communities.gov.uk/) (sign in with GOV.UK
One Login). Scottish data needs none. The token must be either:

- in the environment variable `EPB_API_TOKEN`, or
- in a `.env` file in your project, as `EPB_API_TOKEN=<your token>`. The file is looked for in the directory you run
  from, then in each parent directory, so the project root works from any subdirectory.

From Python, you can also set the variable before the first call that needs it, without writing the token into your
code:

```python
import os
from getpass import getpass

os.environ["EPB_API_TOKEN"] = getpass("England and Wales API token: ")
```

A token already in the environment is used rather than the one in `.env`. This applies to the command line and to the
Python functions alike. Keep `.env` out of version control (add it to your `.gitignore`): the token is personal.

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
