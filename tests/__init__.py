"""
DSS150P - pipeline tests.

Fast, deterministic tests that do not require Docker, Postgres, or Airflow.
Run with: pytest -q
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.config import get_settings
from src.ingest import sha256_of_file, utcnow_iso
from src.transform import build_curated, build_staging, read_all_raw_snapshots
from src.validate import run_curated_checks, run_staging_checks


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def sample_raw() -> pd.DataFrame:
    """Small synthetic frame that matches the source schema we care about."""
    return pd.DataFrame(
        {
            "collision_index": ["A1", "A2", "A3", "A3", "A4"],
            "collision_year": [2023, 2023, 2024, 2024, 2025],
            "collision_ref_no": ["R1", "R2", "R3", "R3", "R4"],
            "collision_severity": [3, 2, 1, 1, 3],
            "date": ["01/06/2023", "15/07/2023", "20/08/2024", "20/08/2024", "05/01/2025"],
            "time": ["08:30", "17:45", "23:00", "23:00", "12:15"],
            "day_of_week": [4, 6, 2, 2, 1],
            "police_force": [1, 1, 48, 48, 1],
            "number_of_vehicles": [2, 1, 3, 3, 1],
            "number_of_casualties": [1, 1, 1, 1, 2],
            "longitude": [-0.12, -0.10, -0.05, -0.05, 0.0],
            "latitude": [51.5, 51.5, 51.6, 51.6, 0.0],
            "speed_limit": [30, 30, 20, 20, 30],
            "road_type": [6, 6, 3, 3, 6],
            "urban_or_rural_area": [1, 1, 1, 1, 1],
            "light_conditions": [1, 4, 1, 1, 1],
            "weather_conditions": [1, 1, 2, 2, 1],
            "road_surface_conditions": [1, 1, 2, 2, 1],
            "junction_detail": [0, -1, 13, 13, 0],
            "junction_control": [-1, -1, 4, 4, -1],
            "pedestrian_crossing": [0, -1, 0, 0, 0],
            "carriageway_hazards": [-1, -1, -1, -1, -1],
            "special_conditions_at_site": [-1, -1, -1, -1, -1],
            "local_authority_district": [1, 1, 2, 2, 1],
            "did_police_officer_attend_scene_of_accident": [1, 1, 1, 1, 1],
            "trunk_road_flag": [2, 2, 2, 2, 2],
            "second_road_class": [-1, -1, -1, -1, -1],
            "second_road_number": [-1, -1, -1, -1, -1],
            "first_road_class": [6, 6, 3, 3, 6],
            "first_road_number": [0, 0, 171, 171, 0],
            "enhanced_severity_collision": [3, 2, 1, 1, 3],
            "collision_injury_based": [1, 1, 1, 1, 1],
            "collision_adjusted_severity_serious": [0.1, 0.2, 1.0, 1.0, 0.1],
            "collision_adjusted_severity_slight": [0.9, 0.8, 0.0, 0.0, 0.9],
            "junction_detail_historic": [-1, -1, -1, -1, -1],
            "pedestrian_crossing_human_control_historic": [-1, -1, -1, -1, -1],
            "pedestrian_crossing_physical_facilities_historic": [-1, -1, -1, -1, -1],
            "carriageway_hazards_historic": [-1, -1, -1, -1, -1],
            "local_authority_ons_district": ["E1", "E1", "E2", "E2", "E1"],
            "local_authority_highway": ["E1", "E1", "E2", "E2", "E1"],
            "local_authority_highway_current": ["E1", "E1", "E2", "E2", "E1"],
            "lsoa_of_accident_location": ["L1", "L1", "L2", "L2", "L1"],
        }
    )


# ----------------------------------------------------------------------
# Ingest utilities
# ----------------------------------------------------------------------
def test_sha256_of_file(tmp_path: Path) -> None:
    p = tmp_path / "hello.txt"
    p.write_bytes(b"hello world")
    expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    assert sha256_of_file(p) == expected


def test_utcnow_iso_format() -> None:
    s = utcnow_iso()
    assert s.endswith("Z")
    assert len(s) == 20


# ----------------------------------------------------------------------
# Transform
# ----------------------------------------------------------------------
def test_staging_deduplicates(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    assert staging["collision_index"].duplicated().sum() == 0
    assert len(staging) == 4


def test_staging_parses_datetime(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    assert staging["collision_datetime"].notna().all()
    assert staging["collision_datetime"].dt.year.between(2023, 2025).all()


def test_staging_nulls_sentinel_codes(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    assert staging["junction_detail"].isna().any()
    assert staging["carriageway_hazards"].isna().all()


def test_staging_nulls_zero_coordinates(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    zero_row = staging[staging["collision_index"] == "A4"]
    assert zero_row["latitude"].isna().all()
    assert zero_row["longitude"].isna().all()


def test_staging_drops_leakage_and_historic(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    for col in [
        "enhanced_severity_collision",
        "collision_injury_based",
        "collision_adjusted_severity_serious",
        "collision_adjusted_severity_slight",
        "junction_detail_historic",
    ]:
        assert col not in staging.columns


def test_curated_derives_is_fatal(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    curated = build_curated(staging)
    assert set(curated["is_fatal"].unique()).issubset({0, 1})
    expected_fatal = (curated["collision_severity"] == 1).astype(int)
    assert (curated["is_fatal"] == expected_fatal).all()


def test_curated_sorted_by_collision_index(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    curated = build_curated(staging)
    assert curated["collision_index"].is_monotonic_increasing


# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------
def test_staging_checks_pass(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    report = run_staging_checks(staging, run_id="test")
    assert not report.failures


def test_curated_checks_pass(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    curated = build_curated(staging)
    report = run_curated_checks(curated, run_id="test")
    assert not report.failures


def test_staging_checks_catch_duplicate(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    staging = pd.concat([staging, staging.iloc[[0]]], ignore_index=True)
    report = run_staging_checks(staging, run_id="test")
    assert any(r.name == "uniqueness" and not r.passed for r in report.results)


def test_curated_checks_catch_bad_reconciliation(sample_raw: pd.DataFrame) -> None:
    staging = build_staging(sample_raw)
    curated = build_curated(staging)
    curated.loc[curated.index[0], "is_fatal"] = 1 - curated.loc[curated.index[0], "is_fatal"]
    report = run_curated_checks(curated, run_id="test")
    assert any(r.name == "reconciliation_is_fatal" and not r.passed for r in report.results)


# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------
def test_settings_loads() -> None:
    s = get_settings()
    assert s.repo_root.exists()
    assert s.source_checksum
    assert s.algorithm
    assert s.random_seed == 42
