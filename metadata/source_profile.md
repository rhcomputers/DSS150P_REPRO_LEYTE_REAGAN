# Source Profile — UK DfT STATS19 Collision Data

## 1. Dataset Identity

| Field | Value |
|-------|-------|
| Identifier | `uk_dft_stats19_collision` |
| Title | UK Department for Transport — STATS19 Road Collision Data (Last 5 Years) |
| Provider | UK Department for Transport (DfT) |
| Source URL | https://data.dft.gov.uk/road-accidents-safety-data/ |
| Access date | 2026-10-13 |
| License | Open Government Licence v3.0 |
| Redistributable | Yes |
| Local filename | `data/source/dft-road-casualty-statistics-collision-last-5-years.csv` |
| Format | CSV, comma-delimited, UTF-8 |
| Size | 97,669,586 bytes (~98 MB) |
| Rows | 513,801 |
| Columns | 44 |
| SHA-256 | `06a03c0e5504175e23ed98e3e0b4767ac61d396c073405a1bddab9339c18f948` |
| Collision years | 2021, 2022, 2023, 2024, 2025 |

## 2. Grain

One row = **one collision**. Not one casualty, not one vehicle.
Confirmed by the natural key `collision_index` (unique) and the DfT specification.

## 3. Column Inventory (44 columns)

### Identifiers and keys
- `collision_index` — unique collision identifier (string)
- `collision_year` — 2021–2025 (int)
- `collision_ref_no` — police force reference (string)
- `police_force` — police force code (int, coded)

### Location
- `location_easting_osgr` — OS grid easting (float, 53 nulls)
- `location_northing_osgr` — OS grid northing (float, 53 nulls)
- `longitude` — WGS84 longitude (float, 53 nulls)
- `latitude` — WGS84 latitude (float, 53 nulls)
- `local_authority_district` — district code (int)
- `local_authority_ons_district` — ONS district code (string)
- `local_authority_highway` — highway authority code (string)
- `local_authority_highway_current` — current highway code (string)
- `lsoa_of_accident_location` — LSOA 2021 code (string)
- `urban_or_rural_area` — coded 1=urban, 2=rural, 3=unallocated

### Time
- `date` — collision date, format `DD/MM/YYYY` (string)
- `time` — collision time, format `HH:MM` (string)
- `day_of_week` — coded 1=Sunday … 7=Saturday

### Severity and casualty
- `collision_severity` — coded **1=fatal, 2=serious, 3=slight**
- `number_of_vehicles` — vehicles involved
- `number_of_casualties` — casualties recorded

### Road environment
- `first_road_class`, `first_road_number`
- `second_road_class`, `second_road_number`
- `road_type`, `speed_limit`
- `junction_detail`, `junction_detail_historic`
- `junction_control`
- `pedestrian_crossing`, `pedestrian_crossing_human_control_historic`, `pedestrian_crossing_physical_facilities_historic`
- `light_conditions`, `weather_conditions`, `road_surface_conditions`
- `special_conditions_at_site`
- `carriageway_hazards`, `carriageway_hazards_historic`
- `trunk_road_flag`
- `did_police_officer_attend_scene_of_accident`

### DfT-derived severity adjustments
- `enhanced_severity_collision` — DfT adjustment code
- `collision_injury_based` — flag
- `collision_adjusted_severity_serious` — modelled proportion serious (float)
- `collision_adjusted_severity_slight` — modelled proportion slight (float)

## 4. Null Profile

| Column(s) | Null count | Notes |
|-----------|-----------|-------|
| `location_easting_osgr`, `location_northing_osgr`, `longitude`, `latitude` | 53 each | Same 53 rows — missing coordinates |
| All other 40 columns | 0 | Complete |

**Implication:** Location-based features must handle 53 missing rows gracefully
(drop or impute). All other columns are safe for strict nullability checks.

## 5. Target Variable — `collision_severity`

| Code | Meaning | Count | Share |
|------|---------|-------|-------|
| 1 | Fatal | 7,553 | 1.47% |
| 2 | Serious | 116,813 | 22.74% |
| 3 | Slight | 389,435 | 75.79% |
| — | Total | 513,801 | 100% |

**Binary derivation for this lab:** `is_fatal = (collision_severity == 1)`

- Positive class: 7,553 rows (1.47%)
- Negative class: 506,248 rows (98.53%)
- **Class imbalance:** severe (~1:67)
- **Mitigation:** `class_weight: balanced` in the model, and report both
  accuracy and F1/precision/recall in `expected_metrics.json`.

## 6. Duplicates and Candidate Keys

- **Candidate key:** `collision_index` — expected unique, one per collision.
- **Duplicate-row check:** to be enforced in the ingestion validation gate.
- **Duplicate business key check:** to be enforced on `(collision_year, collision_ref_no)`.

## 7. Data Quality Risks Identified at Profiling

| Risk | Detail | Mitigation |
|------|--------|------------|
| Missing coordinates | 53 rows | Drop from geo-features; keep for tabular model |
| UK coded fields | Many columns use `-1` for "not recorded" | Treat `-1` as null in staging |
| Date format | `DD/MM/YYYY`, not ISO | Explicit parser, never rely on pandas guessing |
| Severe class imbalance | 1.47% fatal | `class_weight=balanced`, metric suite |
| Historic columns | `*_historic` are legacy duplicates | Document, exclude from model features |
| DfT adjusted severities | Modelled, not raw | Exclude from features (potential leakage) |
| Timestamp quality | `date` + `time` as strings | Parse to single `collision_datetime` in staging |

## 8. Leakage Watchlist

Columns that must **NOT** be used as model features:

- `enhanced_severity_collision`
- `collision_injury_based`
- `collision_adjusted_severity_serious`
- `collision_adjusted_severity_slight`

These are derived from or correlated with the target by DfT post-processing.

## 9. Ownership and Availability

- **Owner:** UK Department for Transport
- **Update frequency:** Annual (each year's STATS19 data published the following autumn)
- **Revisions:** DfT may revise prior years; treat each download as a new snapshot
- **Expected availability:** Public, permanent URL; Open Government Licence permits reuse
- **Redistribution:** Permitted with attribution

## 10. Why This Source Was Chosen for DSS150P

- Single-file, single-sheet CSV — simple immutable source
- Contains a natural binary target (`is_fatal`) at ~1.5% positive rate
- 44 columns give a realistic feature engineering surface
- 5 collision years provide a natural deterministic batch key for incremental simulation
- Public, licensed for redistribution — no PII, no access restrictions
- ~98 MB is large enough to be non-trivial but small enough to run inside Docker + Airflow comfortably