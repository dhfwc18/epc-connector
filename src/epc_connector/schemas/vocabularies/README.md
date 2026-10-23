# schemas/vocabularies

How the free-text building descriptions of domestic EPCs (walls, roof, heating, ...) become typed columns. One file per
described element; `vocabulary.schema.json` defines the format and the `epc_connector.cleaning` package applies it.

## Reading a vocabulary file

Each file lists, for every output field, which phrases give which value:

```json
"construction": {
  "type": "string",
  "values": {
    "sandstone or limestone": ["sandstone or limestone", "sandstone", "tywodfaen"]
  }
}
```

reads "the phrase `sandstone`, or the Welsh `tywodfaen`, gives `walls_construction = "sandstone or limestone"`". A
phrase can appear under several fields: `no insulation (assumed)` gives both `insulation = "none"` and
`insulation_assumed = true`.

- **Booleans** list their phrases under `"true"` and `"false"`. A field no phrase mentions is left empty (null), never
  inferred.
- **Numbers** are read from phrases listed under `number_from`, where `#` stands for the number:
  `"# mm loft insulation"` gives `roof_loft_insulation_thickness = 250` for `250 mm loft insulation`. A numeric field
  can also list fixed `values` (`"100": ["low energy lighting in all fixed outlets"]`).
- **`ignore`** lists phrases that are recognised but give nothing, by reason (e.g. glazing text found in the heating
  description). **`discard`** lists phrases that make the whole description unusable, by reason (placeholder text,
  truncated values, another element's text); the reason is written to `<prefix>_discarded`.
- **`count_systems_by`** (heating): a phrase that gives this field again starts a new system. Only the first system is
  output, with the number of systems in `<prefix>_systems`. **`second_value_field`**: a second, different value of a
  field goes to the named field (`fuel` -> `other_fuel`) instead of being an error.
- Phrases are written in normalised form (below). Non-ASCII source characters are written as JSON `\u` escapes.

## How a description is cleaned

Worked example, England and Wales walls:

| Step                                                    | Result                                                  |
| ------------------------------------------------------- | ------------------------------------------------------- |
| Raw value                                               | `Average thermal transmittance 0.28 W/m&#0178;K`        |
| 1. Elements: English part / Scottish building parts     | one element, unchanged                                  |
| 2. Normalise                                            | `average thermal transmittance 0.28 w/m<U+00B2>k`       |
| 3. Split on commas (unless the whole element is listed) | one phrase                                              |
| 4. Look up; if not listed, replace the number with `#`  | `average thermal transmittance # w/m<U+00B2>k` (listed) |
| Output                                                  | `walls_u_value = 0.28`, `walls_parts = 1`               |

1. **Elements.** England and Wales: `english|welsh` keeps the English; values that alternate English and Welsh phrase by
   phrase keep every English part. Scotland: `main | extension` gives one element per building part; the first part that
   is not discarded is output, and `<prefix>_parts` counts the parts.
2. **Normalise**: HTML entities, lower case, mis-decoded UTF-8, a leading assessment software control code, runs of `?`
   used as separators, apostrophes, `&`, a leading `description:` label, millimetres, `(assumed)` spacing, comma spacing
   and whitespace.
3. **Phrases.** An element listed as a whole is one phrase; otherwise it is split on commas.
4. **Look up** each phrase exactly. A phrase that is not listed, or two phrases giving one field different values, is an
   error: add the phrase to the right list (or to `ignore` or `discard` with a reason) rather than guessing.

## Output columns

| File                                | Columns                                                                                                                                                                                                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `walls.vocabulary.json`             | `walls_construction`, `walls_insulation`, `walls_insulation_assumed`, `walls_as_built`, `walls_u_value`                                                                                                                                                       |
| `roof.vocabulary.json`              | `roof_type`, `roof_thatched`, `roof_insulation`, `roof_insulation_assumed`, `roof_loft_insulation_thickness`, `roof_loft_insulation_or_more`, `roof_u_value`                                                                                                  |
| `floor.vocabulary.json`             | `floor_type`, `floor_insulation`, `floor_insulation_assumed`, `floor_u_value`                                                                                                                                                                                 |
| `windows.vocabulary.json`           | `windows_glazing`, `windows_coverage`                                                                                                                                                                                                                         |
| `glazed-type.vocabulary.json`       | `glazing_type`, `glazing_installed`, `glazing_performance_known`                                                                                                                                                                                              |
| `lighting.vocabulary.json`          | `lighting_low_energy_percent`, `lighting_efficiency`                                                                                                                                                                                                          |
| `main-heating.vocabulary.json`      | `main_heating_system`, `main_heating_emitter`, `main_heating_underfloor_construction`, `main_heating_fuel`, `main_heating_other_fuel`, `main_heating_tariff`, `main_heating_chp`, `main_heating_electricaire`, `main_heating_assumed`, `main_heating_systems` |
| `heating-controls.vocabulary.json`  | one boolean per control (`heating_controls_programmer`, `heating_controls_trvs`, ...), per stated absence (`heating_controls_no_room_thermostat`, ...), `heating_controls_charging`, `heating_controls_applicable`                                            |
| `hot-water.vocabulary.json`         | `hot_water_source`, `hot_water_assumed`, `hot_water_dedicated`, `hot_water_tariff`, `hot_water_cylinder_thermostat`, `hot_water_solar`, `hot_water_waste_water_heat_recovery`, `hot_water_flue_gas_heat_recovery`, `hot_water_boiler_age`, `hot_water_fuel`   |
| `secondary-heating.vocabulary.json` | `secondary_heating_system`, `secondary_heating_fuel`, `secondary_heating_assumed`, `secondary_heating_systems`                                                                                                                                                |
| `main-fuel.vocabulary.json`         | `main_fuel_type`, `main_fuel_community`                                                                                                                                                                                                                       |

Every element also has `<prefix>_parts` (building parts with a description) and `<prefix>_discarded` (the reason, if the
description was unusable).
