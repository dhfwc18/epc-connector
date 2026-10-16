# schemas/source

One JSON Schema (draft 2020-12) per source dataset. Each schema validates **one CSV row exactly as the source delivers
it**: an object of raw string values keyed by column name.

| Schema                                       | Validates a row of                                                |
| -------------------------------------------- | ----------------------------------------------------------------- |
| `england-and-wales/domestic.schema.json`     | E&W domestic certificates CSV (91 columns)                        |
| `england-and-wales/non-domestic.schema.json` | E&W non-domestic certificates CSV (39 columns)                    |
| `england-and-wales/display.schema.json`      | E&W display energy certificates (DEC) CSV (52 columns)            |
| `scotland/domestic.schema.json`              | Scottish domestic CSV, Extended Historic dataset (107 columns)    |
| `scotland/non-domestic.schema.json`          | Scottish non-domestic CSV, Extended Historic dataset (38 columns) |

## What a schema checks

- **Columns**: every expected column is `required` and unknown columns are rejected (`additionalProperties: false`), so
  a renamed, dropped or added column fails validation.
- **Values**: numbers, dates, timestamps, flags and rating bands must match a pattern or enum. Each such rule also
  accepts the source's missing-value markers (`$defs/missing`): `""`, `Data removed` (Scotland) or `NO DATA!`,
  `INVALID!`, `N/A` (England and Wales). Free-text columns are any string.
- **Documentation**: each property carries the official description; Scottish properties also carry the row 2 label as
  `title`.

The Scottish schemas cover the publisher's Extended Historic datasets: every certificate ever lodged, with `HISTORIC`
set to `True` for certificates superseded by a newer one. Scottish CSVs have two header rows: row 1 holds the column
names used as keys, row 2 holds descriptive labels and is not a data row.
