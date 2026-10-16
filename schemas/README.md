# schemas

JSON Schemas (draft 2020-12) used to validate EPC data, and the mappings between them.

```text
schemas/
  source/       one schema per source dataset; validates raw CSV rows exactly as delivered
    england-and-wales/   domestic, non-domestic, display (DEC) certificates
    scotland/            domestic, non-domestic (every certificate ever lodged; superseded ones flagged)
  canonical/    the typed GB domestic record every source is mapped to
  mappings/     declarative source -> canonical mappings, plus the schema that defines the mapping format
```

See the README in each directory for details.
