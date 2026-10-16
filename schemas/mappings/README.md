# schemas/mappings

Declarative mappings from each source dataset (`schemas/source/`) to the canonical record (`schemas/canonical/`).

| Mapping                                            | Source schema                                            |
| -------------------------------------------------- | -------------------------------------------------------- |
| `england-and-wales-domestic.mapping.json`          | `source/england-and-wales/domestic.schema.json`          |
| `scotland-domestic.mapping.json`                   | `source/scotland/domestic.schema.json`                   |
| `scotland-domestic-extended-historic.mapping.json` | `source/scotland/domestic-extended-historic.schema.json` |

`mapping.schema.json` defines the format and the meaning of each step. In short: each canonical field is either a
`const` or taken `from` one source column. The value is trimmed, values in `null_values` become null, then `steps` run
in order (`first_part`, `lower`, `strip_prefix`, `map`, `cast`, `boolean`, `divide`, `round`, `greater_than`, `null_if`,
`regex_extract`). A step that meets a value it cannot handle is an error, so new source values surface instead of being
silently dropped.
