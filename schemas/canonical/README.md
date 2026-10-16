# schemas/canonical

`gb-domestic.schema.json` (JSON Schema draft 2020-12) defines one domestic EPC record for Great Britain, mapped from any
source dataset by `schemas/mappings/`. Values are typed (integers, numbers, booleans, ISO dates), every field is
required, null means the source has no value, and unknown fields are rejected.

| Group               | Fields                                                                                                                                                                            |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Provenance          | `source_dataset`, `country`, `certificate_id`, `uprn`, `superseded`                                                                                                               |
| Dates               | `inspection_date`, `lodgement_date`, `lodged_at`                                                                                                                                  |
| Address             | `address_line_1`, `address_line_2`, `post_town`, `postcode`, `local_authority_name`                                                                                               |
| Ratings             | `current_energy_rating`, `current_energy_efficiency`, `potential_energy_rating`, `potential_energy_efficiency`                                                                    |
| Property            | `property_type`, `built_form`, `tenure`, `total_floor_area`                                                                                                                       |
| Construction age    | `construction_age_band` (as labelled by the source), `construction_year_min`, `construction_year_max`                                                                             |
| Building elements   | `*_description` and `*_energy_efficiency` for main heating, heating controls, hot water, walls, roof, floor and windows; `lighting_energy_efficiency`; `glazed_type`; `main_fuel` |
| Renewables          | `has_solar_pv`, `has_solar_water_heating`                                                                                                                                         |
| Emissions and costs | `co2_emissions_current` (tonnes per year), `heating_cost_current`, `hot_water_cost_current`, `lighting_cost_current` (GBP per year)                                               |

Notes:

- Construction age bands differ between nations. Compare properties with `construction_year_min` and
  `construction_year_max`, which are null where a band is open-ended.
- Descriptions and efficiency ratings hold the first element of a building part (and the English text where the source
  is bilingual).
- `superseded` is only known for Scottish data; it is null for England and Wales.
- Address fields are subject to Royal Mail and Ordnance Survey licence terms.
