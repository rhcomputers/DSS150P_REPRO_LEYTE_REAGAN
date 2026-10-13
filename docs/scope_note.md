# Scope Note — DSS150P Individual Laboratory Activity #4

## Context
The original group project (THETA — cross-city road crash pipeline) was a
data harmonization project: it ingested three crash sources (Chicago, NYC,
UK STATS19), profiled them, transformed them into a common schema, validated
them, and published a single `fact_crash` table plus partitioned Parquet.
It did NOT include a predictive analytics step.

## Why This Project Adds a Predictive Layer
DSS150P Laboratory Activity #4 requires reproducing a *predictive analytics*
project. Because the original group work had no model, this individual
project adds a small, defensible predictive layer on top of the harmonized
data product, in line with the lab's statement that:

> "The predictive model is the downstream consumer of the engineered data product."

## Source Dataset
Single authoritative source for this lab:
- File: `dft-road-casualty-statistics-collision-last-5-years.csv`
- Provider: UK Department for Transport (DfT), STATS19
- Format: CSV
- Size: ~98 MB
- Access: publicly redistributable (Open Government Licence v3.0)

## Prediction Task
- Type: Binary classification
- Target: `is_fatal` (derived from `collision_severity`)
- Features: date/time components, location, road type, weather, road surface,
  urban/rural flag, speed limit, and other non-leaky fields present in the
  source.
- Model family: to be finalized (baseline: RandomForest / LogisticRegression)
- Fixed random seed: 42 (documented in `config/settings.yml`)

## Reuse and Attribution
- Dataset: reused from the THETA group project (public UK DfT data)
- Problem framing: harmonization work reused as the data product; the
  predictive framing and all engineering code in this repository are the
  individual work of LEYTE, REAGAN.

## What Is NOT Reused
- No group notebooks, source code, DAGs, Docker files, or configuration
  files are copied. Only the raw public dataset and the problem context
  (cross-city crash data, STATS19) are carried over.