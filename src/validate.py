"""
Validation module for the DSS150P reproduction pipeline.

Two validation gates, matching the data contract:

1. staging_checks — run per raw snapshot after staging transform.
   Purpose: schema, nullability, uniqueness, accepted values, ranges,
   date logic, referential integrity, row counts.

2. curated_checks — run after harmonization into the curated table.
   Purpose: schema, uniqueness, reconciliation, timestamps, partitions.

Any violation of severity "fail" halts the pipeline. Severity "warn"
is logged and recorded but does not halt.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from src.config import Settings, configure_logging, get_settings

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Check result model
# ----------------------------------------------------------------------
@dataclass
class CheckResult:
    name: str
    layer: str
    severity: str         
    passed: bool
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    pipeline_run_id: str
    layer: str
    checked_at_utc: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "fail"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == "warn"]

    @property
    def passes(self) -> list[CheckResult]:
        return [r for r in self.results if r.passed]

    def add(self, r: CheckResult) -> None:
        self.results.append(r)
        status = "PASS" if r.passed else r.severity.upper()
        log.info("[%s] %s.%s %s", status, r.layer, r.name, f"- {r.detail}" if r.detail else "")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "layer": self.layer,
            "checked_at_utc": self.checked_at_utc,
            "summary": {
                "pass": len(self.passes),
                "warn": len(self.warnings),
                "fail": len(self.failures),
            },
            "results": [r.to_dict() for r in self.results],
        }

    def raise_if_failed(self) -> None:
        if self.failures:
            names = ", ".join(f.name for f in self.failures)
            raise AssertionError(f"{len(self.failures)} check(s) failed at {self.layer}: {names}")


# ----------------------------------------------------------------------
# Individual checks
# ----------------------------------------------------------------------
def _check(name: str, layer: str, severity: str, fn: Callable[[], tuple[bool, str]]) -> CheckResult:
    try:
        ok, detail = fn()
        return CheckResult(name=name, layer=layer, severity=severity, passed=ok, detail=detail)
    except Exception as exc:
        return CheckResult(
            name=name, layer=layer, severity=severity, passed=False,
            detail=f"raised {type(exc).__name__}: {exc}",
        )


# ----------------------------------------------------------------------
# Staging checks
# ----------------------------------------------------------------------
def run_staging_checks(df: pd.DataFrame, run_id: str) -> ValidationReport:
    report = ValidationReport(
        pipeline_run_id=run_id,
        layer="staging",
        checked_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    required = ["collision_index", "collision_severity", "date", "time", "collision_datetime"]
    def schema_check() -> tuple[bool, str]:
        missing = [c for c in required if c not in df.columns]
        return (len(missing) == 0, f"missing={missing}" if missing else f"all {len(required)} present")
    report.add(_check("schema", "staging", "fail", schema_check))

    def nullability_check() -> tuple[bool, str]:
        bad = {}
        for c in ["collision_index", "collision_severity", "collision_datetime"]:
            n = int(df[c].isna().sum())
            if n:
                bad[c] = n
        return (len(bad) == 0, f"nulls={bad}" if bad else "no nulls in required columns")
    report.add(_check("nullability", "staging", "fail", nullability_check))

    def uniqueness_check() -> tuple[bool, str]:
        d = int(df["collision_index"].duplicated().sum())
        return (d == 0, f"duplicates={d}")
    report.add(_check("uniqueness", "staging", "fail", uniqueness_check))

    def accepted_severity() -> tuple[bool, str]:
        bad = set(df["collision_severity"].dropna().unique()) - {1, 2, 3}
        return (len(bad) == 0, f"unexpected severity codes={sorted(bad)}")
    report.add(_check("accepted_values_collision_severity", "staging", "fail", accepted_severity))

    def range_vehicles() -> tuple[bool, str]:
        s = df["number_of_vehicles"]
        bad = int(((s < 1) | (s > 100)).sum())
        return (bad == 0, f"out_of_range={bad}")
    report.add(_check("range_number_of_vehicles", "staging", "fail", range_vehicles))

    def date_logic() -> tuple[bool, str]:
        years = df["collision_datetime"].dt.year
        bad = int((~years.between(2021, 2025)).sum())
        return (bad == 0, f"outside 2021-2025={bad}")
    report.add(_check("date_logic", "staging", "fail", date_logic))

    def coordinate_ranges() -> tuple[bool, str]:
        bad_lat = int(((df["latitude"].notna()) & (~df["latitude"].between(49, 61))).sum())
        bad_lon = int(((df["longitude"].notna()) & (~df["longitude"].between(-8, 2))).sum())
        return (bad_lat == 0 and bad_lon == 0, f"bad_lat={bad_lat} bad_lon={bad_lon}")
    report.add(_check("coordinate_ranges", "staging", "warn", coordinate_ranges))

    def row_count_check() -> tuple[bool, str]:
        return (len(df) > 0, f"rows={len(df)}")
    report.add(_check("row_count", "staging", "warn", row_count_check))

    return report


# ----------------------------------------------------------------------
# Curated checks
# ----------------------------------------------------------------------
def run_curated_checks(df: pd.DataFrame, run_id: str) -> ValidationReport:
    report = ValidationReport(
        pipeline_run_id=run_id,
        layer="curated",
        checked_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    def schema_check() -> tuple[bool, str]:
        required = ["collision_index", "collision_severity", "is_fatal", "collision_datetime", "year", "month"]
        missing = [c for c in required if c not in df.columns]
        return (len(missing) == 0, f"missing={missing}" if missing else f"all {len(required)} present")
    report.add(_check("schema", "curated", "fail", schema_check))

    def uniqueness_check() -> tuple[bool, str]:
        d = int(df["collision_index"].duplicated().sum())
        return (d == 0, f"duplicates={d}")
    report.add(_check("uniqueness", "curated", "fail", uniqueness_check))

    def reconciliation_check() -> tuple[bool, str]:
        mismatch = int((df["is_fatal"] != (df["collision_severity"] == 1).astype(int)).sum())
        return (mismatch == 0, f"mismatches={mismatch}")
    report.add(_check("reconciliation_is_fatal", "curated", "fail", reconciliation_check))

    def timestamps_check() -> tuple[bool, str]:
        bad = int(df["collision_datetime"].isna().sum())
        return (bad == 0, f"null_datetimes={bad}")
    report.add(_check("timestamps", "curated", "fail", timestamps_check))

    def partitions_check() -> tuple[bool, str]:
        bad = int(df["year"].isna().sum())
        return (bad == 0, f"rows_missing_year={bad}")
    report.add(_check("partitions", "curated", "fail", partitions_check))

    return report


# ----------------------------------------------------------------------
# Persistence
# ----------------------------------------------------------------------
def write_report(report: ValidationReport, settings: Settings) -> Path:
    out_dir = settings.outputs_dir / "validation"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{report.layer}_{report.pipeline_run_id}.json"
    path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return path


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _cli() -> None:
    parser = argparse.ArgumentParser(description="DSS150P validation")
    parser.add_argument("--layer", choices=["staging", "curated"], required=True)
    parser.add_argument("--run-id", default="manual")
    args = parser.parse_args()

    s = get_settings()
    configure_logging(s)

    if args.layer == "staging":
        df = pd.read_parquet(s.staging_dir / "staging.parquet")
        report = run_staging_checks(df, args.run_id)
    else:
        df = pd.read_parquet(s.curated_dir / "curated.parquet")
        report = run_curated_checks(df, args.run_id)

    path = write_report(report, s)
    print(json.dumps(report.to_dict()["summary"], indent=2))
    print(f"report: {path}")
    report.raise_if_failed()


if __name__ == "__main__":
    _cli()