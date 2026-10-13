"""
Storage module for the DSS150P reproduction pipeline.

Responsibilities:
- Materialize curated data in three formats: CSV, JSONL, Parquet.
- Benchmark read/write size and time for each format (recorded to
  docs/benchmark_report.md).
- Write partitioned Parquet by (source_id, year, month).
- Load curated data into PostgreSQL using UPSERT on (collision_index)
  so reruns are idempotent.
- Create Postgres schema from sql/schema.sql.

Nothing in this module reads raw or staging data. It only consumes the
curated DataFrame produced by src/transform.py.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import Settings, configure_logging, get_settings

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Format benchmark
# ----------------------------------------------------------------------
@dataclass
class FormatBenchmark:
    format: str
    write_seconds: float
    read_seconds: float
    size_bytes: int
    rows: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def benchmark_formats(df: pd.DataFrame, settings: Settings, sample_rows: int = 8924) -> list[FormatBenchmark]:
    """Write and read the same sample in each format, record size and time."""
    sample = df.head(sample_rows).copy()
    out_dir = settings.outputs_dir / "benchmark"
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[FormatBenchmark] = []

    # CSV
    t0 = time.perf_counter()
    p = out_dir / "sample.csv"
    sample.to_csv(p, index=False)
    w = time.perf_counter() - t0
    t0 = time.perf_counter()
    pd.read_csv(p)
    r = time.perf_counter() - t0
    results.append(FormatBenchmark("csv", w, r, p.stat().st_size, len(sample)))

    # JSONL
    t0 = time.perf_counter()
    p = out_dir / "sample.jsonl"
    sample.to_json(p, orient="records", lines=True)
    w = time.perf_counter() - t0
    t0 = time.perf_counter()
    pd.read_json(p, orient="records", lines=True)
    r = time.perf_counter() - t0
    results.append(FormatBenchmark("jsonl", w, r, p.stat().st_size, len(sample)))

    # Parquet
    t0 = time.perf_counter()
    p = out_dir / "sample.parquet"
    sample.to_parquet(p, index=False)
    w = time.perf_counter() - t0
    t0 = time.perf_counter()
    pd.read_parquet(p)
    r = time.perf_counter() - t0
    results.append(FormatBenchmark("parquet", w, r, p.stat().st_size, len(sample)))

    return results


def write_benchmark_report(results: list[FormatBenchmark], settings: Settings) -> Path:
    path = settings.repo_root / "docs" / "benchmark_report.md"
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Format Benchmark Report",
        "",
        f"Sample size: {results[0].rows:,} rows",
        "",
        "| Format | Write (s) | Read (s) | Size (bytes) | Size (MB) |",
        "|--------|-----------|----------|--------------|-----------|",
    ]
    for r in results:
        lines.append(
            f"| {r.format} | {r.write_seconds:.3f} | {r.read_seconds:.3f} | "
            f"{r.size_bytes:,} | {r.size_bytes / 1_048_576:.2f} |"
        )

    lines += [
        "",
        "## Decision",
        "",
        "- **Parquet** is used for everything the pipeline writes (staging, curated, "
        "partitioned, model outputs). It has the smallest footprint and fastest reads.",
        "- **CSV** is used only for source exports and interoperability with tools "
        "that cannot read Parquet.",
        "- **JSONL** is used only for API-shaped outputs and manifests where line "
        "records help with streaming.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# ----------------------------------------------------------------------
# Partitioned Parquet
# ----------------------------------------------------------------------
def write_partitioned_parquet(df: pd.DataFrame, settings: Settings) -> Path:
    """Write curated data as Parquet partitioned by year/month."""
    out_dir = settings.partitioned_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["source_id"] = "uk_stats19"

    df.to_parquet(
        out_dir,
        engine="pyarrow",
        partition_cols=["source_id", "year", "month"],
        index=False,
    )

    partition_count = sum(1 for _ in out_dir.rglob("*.parquet"))
    log.info("Wrote %d partitions under %s", partition_count, out_dir)
    return out_dir


def read_partition(settings: Settings, source_id: str, year: int, month: int) -> pd.DataFrame:
    """Read a single partition — the pruning example used in the report."""
    path = settings.partitioned_dir / f"source_id={source_id}" / f"year={year}" / f"month={month}"
    if not path.exists():
        raise FileNotFoundError(f"No partition at {path}")
    return pd.read_parquet(path)


# ----------------------------------------------------------------------
# PostgreSQL
# ----------------------------------------------------------------------
def _engine(settings: Settings) -> Engine:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "dss150p")
    user = os.getenv("POSTGRES_USER", "dss150p_user")
    pwd = os.getenv("POSTGRES_PASSWORD", "change_me_local_only")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, future=True)


def apply_schema(settings: Settings) -> None:
    """Run sql/schema.sql against the target database."""
    sql_path = settings.repo_root / "sql" / "schema.sql"
    ddl = sql_path.read_text(encoding="utf-8")
    engine = _engine(settings)
    with engine.begin() as conn:
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            conn.execute(text(stmt))
    log.info("Applied schema from %s", sql_path)


def load_curated_to_postgres(df: pd.DataFrame, settings: Settings, table: str | None = None) -> int:
    """UPSERT curated rows on collision_index. Idempotent across reruns."""
    target = table or settings.postgres_tables["curated"]
    engine = _engine(settings)

    cols = [
        "collision_index", "collision_year", "collision_severity", "is_fatal",
        "collision_datetime", "longitude", "latitude", "police_force",
        "number_of_vehicles", "number_of_casualties", "speed_limit",
        "road_type", "urban_or_rural_area", "light_conditions",
        "weather_conditions", "road_surface_conditions",
    ]
    present = [c for c in cols if c in df.columns]
    to_load = df[present].copy()

    for c in ["collision_datetime"]:
        if c in to_load.columns:
            to_load[c] = pd.to_datetime(to_load[c])
    for c in to_load.select_dtypes(include="Int64").columns:
        to_load[c] = to_load[c].astype("float64")

    to_load.to_sql(
        f"{settings.postgres_schema}.{target}_staging",
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10000,
    )

    upsert_sql = f"""
        INSERT INTO {settings.postgres_schema}.{target} ({", ".join(present)})
        SELECT {", ".join(present)} FROM {settings.postgres_schema}.{target}_staging
        ON CONFLICT (collision_index) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in present if c != "collision_index")};
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE {settings.postgres_schema}.{target}_staging;"))

    log.info("Loaded %d rows into %s.%s", len(to_load), settings.postgres_schema, target)
    return len(to_load)


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _cli() -> None:
    parser = argparse.ArgumentParser(description="DSS150P storage")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--partition", action="store_true")
    parser.add_argument("--postgres", action="store_true")
    args = parser.parse_args()

    s = get_settings()
    configure_logging(s)

    curated = pd.read_parquet(s.curated_dir / "curated.parquet")

    if args.benchmark:
        results = benchmark_formats(curated, s)
        path = write_benchmark_report(results, s)
        print(json.dumps([r.to_dict() for r in results], indent=2))
        print(f"report: {path}")

    if args.partition:
        path = write_partitioned_parquet(curated, s)
        print(f"partitions: {path}")

    if args.postgres:
        apply_schema(s)
        n = load_curated_to_postgres(curated, s)
        print(f"loaded: {n}")


if __name__ == "__main__":
    _cli()