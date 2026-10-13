# Reproducibility Report — DSS150P

## Scope

This report documents the evidence that the pipeline reproduces the declared baseline when run on a clean machine from the same source dataset.

## 1. Source identity

| Field | Value |
|-------|-------|
| Filename | `dft-road-casualty-statistics-collision-last-5-years.csv` |
| Provider | UK Department for Transport (DfT) |
| License | Open Government Licence v3.0 |
| Rows | 513,801 |
| Columns | 44 |
| SHA-256 | `06a03c0e5504175e23ed98e3e0b4767ac61d396c073405a1bddab9339c18f948` |

Any reviewer can recompute the checksum and compare. If it differs, the source is not the same and reproducibility cannot be claimed.

## 2. Environment identity

| Component | Version |
|-----------|---------|
| Python | 3.11 |
| pandas | 2.2.3 |
| numpy | 2.1.3 |
| scikit-learn | 1.5.2 |
| pyarrow | 17.0.0 |
| PostgreSQL | 16 |
| Airflow | 2.9.3 |
| Docker Compose | v2 |

All Python dependencies are pinned in `requirements.txt`.

## 3. Split reproducibility

The train/test split is:
- stratified on `is_fatal`,
- seeded with `random_state=42`,
- sorted by `collision_index` before splitting.

Hashes of the exact index sets assigned to each side are frozen in `metadata/split_manifest.json`:

| Artifact | Value |
|----------|-------|
| Train rows | 411,040 |
| Test rows | 102,761 |
| Train positives | 6,042 |
| Test positives | 1,511 |

Any reviewer who re-runs `src/train.py` with the same curated data will compute identical hashes.

## 4. Metric reproducibility

The baseline metrics were frozen on the first authoritative run. Subsequent runs must reproduce each metric within the declared tolerance (±0.02).

| Metric | Frozen baseline | Latest run | Delta | Within tolerance |
|--------|----------------|------------|-------|------------------|
| accuracy | 0.7692 | 0.7692 | 0.0000 | yes |
| precision | 0.0426 | 0.0426 | 0.0000 | yes |
| recall | 0.6843 | 0.6843 | 0.0000 | yes |
| f1 | 0.0802 | 0.0802 | 0.0000 | yes |
| roc_auc | 0.8079 | 0.8079 | 0.0000 | yes |

See `outputs/metrics.json` and `metadata/expected_metrics.json`.

## 5. Rerun safety

- **Ingestion.** The `state/ingest_checkpoint.json` file records committed batches. A second run skips committed batches (verified: 5 batches skipped on rerun).
- **Transformation.** Reads all raw snapshots and deduplicates on `collision_index`. A rerun produces byte-identical staging and curated output.
- **Postgres load.** Uses `ON CONFLICT (collision_index) DO UPDATE`. A second load leaves counts unchanged.
- **Validation.** Report files are overwritten, not appended. No duplicates accumulate.
- **Prediction output.** `outputs/predictions.csv` is overwritten. Row count stable at 102,761.

## 6. Clean-clone evidence

The sequence in Section 2 of the README has been executed on:

- A Windows 11 machine with Docker Desktop 4.x, Python 3.11, fresh clone, no prior cached images.
- All stages completed successfully.
- Metrics matched the baseline within tolerance.
- No Python source files required editing.

## 7. Known non-determinism

None. Every source of variability (data, seed, split, dependencies) is pinned. The RandomForest implementation is seeded and reproducible on the same scikit-learn version. Cross-platform floating-point differences are within the declared ±0.02 tolerance.

## 8. Limitations

- The source refresh path is manual (as it was in the original group project).
- F1 and precision are low due to severe class imbalance; this is a property of the problem, not a defect.
- Only the UK source is used; the original group project also harmonized Chicago and NYC, which are out of scope for this individual lab.

## 9. Reviewer checklist

A reviewer can verify reproducibility by:

1. Cloning the repository.
2. Placing the source dataset at `data/source/` and confirming its checksum.
3. Running `python -m src.cli all`.
4. Comparing `outputs/metrics.json` against `metadata/expected_metrics.json`.
5. Running `pytest -q` and confirming all tests pass.

Every one of these steps is scriptable and non-interactive.
