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
deletes it. In CI or scripts, where nothing can be typed, set `EPC_CONNECTOR_API_TOKEN` instead; it takes precedence
over the saved token.

**Python.** Pass the token explicitly:

```python
import os

import epc_connector as epc

# If you use a .env file load it first (python-dotenv):
# from dotenv import load_dotenv
# load_dotenv()
token = os.environ["EPC_CONNECTOR_API_TOKEN"]
homes = epc.load_domestic_certificates(api_token=token)
epc.update(api_token=token)
```

Without `api_token`, the functions use `EPC_CONNECTOR_API_TOKEN`, then the token saved by the command line.

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

Data goes to your per-user data directory, so one setup (about 6 GB) serves every project on the machine:

| System  | Directory                                     |
| ------- | --------------------------------------------- |
| Linux   | `~/.local/share/epc-connector`                |
| macOS   | `~/Library/Application Support/epc-connector` |
| Windows | `C:\Users\<user>\AppData\Local\epc-connector` |

It is a data directory rather than a cache one, so cache clean-ups leave it alone. Running `setup` again only downloads
what is missing (`--force` downloads everything again); `update` refreshes what is there. `epc-connector status` shows
the directory and what is in it.

To keep it somewhere else, for example on a larger drive, in a shared directory on a server, or in a directory a CI
cache keeps, set `EPC_CONNECTOR_DATA_DIR`, or pass `--data-dir` (`data_dir=` in Python), which takes precedence. A
folder the package creates gets a `.gitignore` (`*`) and a `CACHEDIR.TAG`, so git and backup tools skip it even if it is
inside a repository. Only one run at a time downloads or rebuilds files in a data directory; a second run waits, then
uses what the first left, so nothing is downloaded twice. Downloads are staged in `.staging/` inside the data directory,
which is removed when each step ends; `--staging-dir` puts it elsewhere.

## Environment variables

| Variable                   | What it sets                                                                  |
| -------------------------- | ----------------------------------------------------------------------------- |
| `EPC_CONNECTOR_DATA_DIR`   | Moves the data directory, unless `--data-dir` / `data_dir=` is given          |
| `EPC_CONNECTOR_API_TOKEN`  | England and Wales API token, unless `api_token=` is given; for CI and scripts |
| `EPC_CONNECTOR_TOKEN_PATH` | Where the command line saves the API token                                    |

They are read when needed, so setting one while a program runs takes effect. Surrounding spaces are ignored and an empty
value counts as unset. Any other `EPC_CONNECTOR_*` variable is reported as unknown, which catches typos.

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
file; non-domestic and display data are never downloaded by it. A first download (several GB, which can take an hour or
more) is announced with a warning; pass `download_if_missing=False` to get a `NotSetUpError` instead, for example in CI.
`force_download=True` downloads the data again. It warns when an origin's data is more than three months old, and when
GB data holds one origin only (for example without an [England and Wales API token](#england-and-wales-api-token)).
Every error the package raises on purpose is an `EpcConnectorError`.

Only the names exported from `epc_connector` (`load_domestic_certificates`, `update`, `status`, `DatasetStatus`,
`Origin`, `Segment` and the errors) are the public API. Everything else lives in underscore-prefixed modules such as
`epc_connector._sources`, which are internal and may change in any release.
