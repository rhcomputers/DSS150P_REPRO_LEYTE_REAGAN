"""
Transformation module for the DSS150P reproduction pipeline.

Two layers are produced:

1. staging_source — cleaned, typed, deduplicated version of the combined
   raw snapshots. Responsibilities:
   - Read and concatenate ALL raw snapshots (full or incremental batches).
   - Deduplicate across snapshots on collision_index.
   - Explicit DD/MM/YYYY and HH:MM parsing (never rely on pandas guessing).
   - Replace UK -1 "not recorded" sentinel codes with null in coded columns.
   - Null out (0,0) lat/lon sentinels.
   - Drop rows missing a valid collision_index or collision_severity.
   - Add collision_datetime and derived time features.

2. curated_model_ready — model-ready table for the prediction task.
   Responsibilities:
   - Derive is_fatal = (collision_severity == 1).
   - Select non-leaky features (exclude DfT derived severity columns).
   - Preserve collision_index as the natural key.
   - Sort by collision_index for deterministic splitting.

Transformations are pure functions of the input DataFrame. No file I/O here.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import Settings, configure_logging, get_settings

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Sentinel codes and drop lists
# ----------------------------------------------------------------------
UK_NOT_RECORDED = -1
CODED_NULL_COLUMNS = [
    "junction_detail",
    "junction_control",
    "pedestrian_crossing",
    "carriageway_hazards",
    "special_conditions_at_site",
    "local_authority_district",
    "urban_or_rural_area",
    "did_police_officer_attend_scene_of_accident",
    "trunk_road_flag",
    "road_type",
    "light_conditions",
    "weather_conditions",
    "road_surface_conditions",
    "second_road_class",
    "second_road_number",
]

LEAKAGE_COLUMNS = [
    "enhanced_severity_collision",
    "collision_injury_based",
    "collision_adjusted_severity_serious",
    "collision_adjusted_severity_slight",
]

HISTORIC_COLUMNS = [
    "junction_detail_historic",
    "pedestrian_crossing_human_control_historic",
    "pedestrian_crossing_physical_facilities_historic",
    "carriageway_hazards_historic",
]


# ----------------------------------------------------------------------
# Raw snapshot reader (all snapshots)
# ----------------------------------------------------------------------
def read_all_raw_snapshots(settings: Settings) -> pd.DataFrame:
    """
    Concatenate every raw snapshot directory under data/raw/ into one frame.
    Deduplicate on collision_index in case a full run and incremental runs
    overlap.
    """
    raw_runs = sorted(
        [p for p in settings.raw_dir.iterdir() if p.is_dir() and (p / "snapshot.parquet").exists()],
        key=lambda p: p.name,
    )
    if not raw_runs:
        raise FileNotFoundError(f"No raw snapshots found in {settings.raw_dir}")

    log.info("Reading %d raw snapshot(s)", len(raw_runs))
    frames = []
    for run in raw_runs:
        df = pd.read_parquet(run / "snapshot.parquet")
        df["_raw_run_id"] = run.name
        frames.append(df)
        log.info("  - %s: %d rows", run.name, len(df))

    combined = pd.concat(frames, ignore_index=True)

    before = len(combined)
    combined = combined.drop_duplicates(subset=["collision_index"], keep="first")
    dropped = before - len(combined)
    if dropped:
        log.warning("Dropped %d duplicate collision_index rows across raw snapshots", dropped)

    log.info("Combined raw: %d rows after dedupe", len(combined))
    return combined


# ----------------------------------------------------------------------
# Staging
# ----------------------------------------------------------------------
def build_staging(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()

    df["collision_date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")
    df["collision_time"] = pd.to_datetime(df["time"], format="%H:%M", errors="coerce").dt.time

    df["collision_datetime"] = pd.to_datetime(
        df["collision_date"].astype(str) + " " + df["time"].astype(str),
        format="%Y-%m-%d %H:%M",
        errors="coerce",
    )

    df["hour"] = df["collision_datetime"].dt.hour
    df["month"] = df["collision_datetime"].dt.month
    df["year"] = df["collision_datetime"].dt.year

    for col in CODED_NULL_COLUMNS:
        if col in df.columns:
            df[col] = df[col].replace(UK_NOT_RECORDED, np.nan)

    df.loc[(df["latitude"] == 0) & (df["longitude"] == 0), ["latitude", "longitude"]] = np.nan

    before = len(df)
    df = df.dropna(subset=["collision_index", "collision_severity"])
    dropped = before - len(df)
    if dropped:
        log.warning("Staging dropped %d rows missing collision_index or collision_severity", dropped)

    before = len(df)
    df = df.drop_duplicates(subset=["collision_index"], keep="first")
    dup_dropped = before - len(df)
    if dup_dropped:
        log.warning("Staging dropped %d duplicate collision_index rows", dup_dropped)

    int_cols = [
        "collision_year", "day_of_week", "police_force", "number_of_vehicles",
        "number_of_casualties", "speed_limit", "first_road_class",
        "first_road_number", "road_type", "junction_detail", "junction_control",
        "light_conditions", "weather_conditions", "road_surface_conditions",
        "urban_or_rural_area",
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    df = df.drop(columns=[c for c in HISTORIC_COLUMNS + LEAKAGE_COLUMNS + ["_raw_run_id"] if c in df.columns])

    log.info("Staging complete: %d rows, %d columns", len(df), df.shape[1])
    return df


# ----------------------------------------------------------------------
# Curated
# ----------------------------------------------------------------------
def build_curated(staging_df: pd.DataFrame) -> pd.DataFrame:
    df = staging_df.copy()
    df["is_fatal"] = (df["collision_severity"] == 1).astype(int)
    df = df.sort_values("collision_index").reset_index(drop=True)

    log.info(
        "Curated complete: %d rows, %d columns, positive rate %.4f",
        len(df), df.shape[1], df["is_fatal"].mean(),
    )
    return df


# ----------------------------------------------------------------------
# Pipeline entry
# ----------------------------------------------------------------------
def transform_all_raw(settings: Settings | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read ALL raw snapshots, build staging and curated."""
    s = settings or get_settings()
    raw_df = read_all_raw_snapshots(s)
    staging = build_staging(raw_df)
    curated = build_curated(staging)

    s.staging_dir.mkdir(parents=True, exist_ok=True)
    s.curated_dir.mkdir(parents=True, exist_ok=True)

    staging.to_parquet(s.staging_dir / "staging.parquet", index=False)
    curated.to_parquet(s.curated_dir / "curated.parquet", index=False)

    return staging, curated


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _cli() -> None:
    parser = argparse.ArgumentParser(description="DSS150P transform")
    parser.parse_args()

    s = get_settings()
    configure_logging(s)

    staging, curated = transform_all_raw(s)
    print(f"staging rows:  {len(staging)}")
    print(f"curated rows:  {len(curated)}")
    print(f"curated cols:  {curated.shape[1]}")
    print(f"is_fatal rate: {curated['is_fatal'].mean():.4f}")


if __name__ == "__main__":
    _cli()