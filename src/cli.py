"""
Unified CLI for the DSS150P reproduction pipeline.

Usage:
    python -m src.cli ingest --mode full
    python -m src.cli ingest --mode incremental
    python -m src.cli transform
    python -m src.cli validate --layer staging
    python -m src.cli validate --layer curated
    python -m src.cli storage --benchmark --partition
    python -m src.cli train
    python -m src.cli evaluate
    python -m src.cli all
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from src.config import configure_logging, get_settings
from src.evaluate import evaluate
from src.ingest import ingest
from src.storage import benchmark_formats, write_benchmark_report, write_partitioned_parquet
from src.train import train
from src.transform import transform_all_raw
from src.validate import run_curated_checks, run_staging_checks, write_report

log = logging.getLogger(__name__)


def _run_ingest(args) -> None:
    s = get_settings()
    audits = ingest(s, force=args.force)
    print(json.dumps([a.__dict__ for a in audits], indent=2, default=str))


def _run_transform(_args) -> None:
    s = get_settings()
    staging, curated = transform_all_raw(s)
    print(f"staging rows:  {len(staging)}")
    print(f"curated rows:  {len(curated)}")
    print(f"is_fatal rate: {curated['is_fatal'].mean():.4f}")


def _run_validate(args) -> None:
    import pandas as pd
    s = get_settings()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.layer == "staging":
        df = pd.read_parquet(s.staging_dir / "staging.parquet")
        report = run_staging_checks(df, run_id)
    else:
        df = pd.read_parquet(s.curated_dir / "curated.parquet")
        report = run_curated_checks(df, run_id)
    path = write_report(report, s)
    print(json.dumps(report.to_dict()["summary"], indent=2))
    print(f"report: {path}")
    report.raise_if_failed()


def _run_storage(args) -> None:
    import pandas as pd
    s = get_settings()
    curated = pd.read_parquet(s.curated_dir / "curated.parquet")

    if args.benchmark:
        results = benchmark_formats(curated, s)
        path = write_benchmark_report(results, s)
        print(json.dumps([r.to_dict() for r in results], indent=2))
        print(f"benchmark report: {path}")

    if args.partition:
        path = write_partitioned_parquet(curated, s)
        print(f"partitions: {path}")

    if args.postgres:
        from src.storage import apply_schema, load_curated_to_postgres
        apply_schema(s)
        n = load_curated_to_postgres(curated, s)
        print(f"loaded to postgres: {n}")


def _run_train(_args) -> None:
    s = get_settings()
    info = train(s)
    print(json.dumps(info, indent=2))


def _run_evaluate(_args) -> None:
    s = get_settings()
    result = evaluate(s)
    print(json.dumps(result, indent=2))


def _run_all(_args) -> None:
    """End-to-end: ingest -> transform -> validate -> storage -> train -> evaluate."""
    s = get_settings()
    log.info("=== STAGE 1/6: ingest (full) ===")
    ingest(s, force=False)

    log.info("=== STAGE 2/6: transform ===")
    staging, curated = transform_all_raw(s)
    log.info("curated rows: %d", len(curated))

    log.info("=== STAGE 3/6: validate ===")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    r1 = run_staging_checks(staging, run_id)
    write_report(r1, s)
    r1.raise_if_failed()
    r2 = run_curated_checks(curated, run_id)
    write_report(r2, s)
    r2.raise_if_failed()

    log.info("=== STAGE 4/6: storage (benchmark + partition) ===")
    results = benchmark_formats(curated, s)
    write_benchmark_report(results, s)
    write_partitioned_parquet(curated, s)

    log.info("=== STAGE 5/6: train ===")
    train(s)

    log.info("=== STAGE 6/6: evaluate ===")
    result = evaluate(s)
    log.info("evaluation complete: %s", result["comparison"]["status"])

    print(json.dumps({"status": "complete", "comparison": result["comparison"]["status"]}, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(prog="src.cli", description="DSS150P pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Run ingestion")
    p_ingest.add_argument("--mode", choices=["full", "incremental"], default=None)
    p_ingest.add_argument("--force", action="store_true")
    p_ingest.set_defaults(func=_run_ingest)

    p_transform = sub.add_parser("transform", help="Run staging + curated transforms")
    p_transform.set_defaults(func=_run_transform)

    p_validate = sub.add_parser("validate", help="Run validation gate")
    p_validate.add_argument("--layer", choices=["staging", "curated"], required=True)
    p_validate.set_defaults(func=_run_validate)

    p_storage = sub.add_parser("storage", help="Storage operations")
    p_storage.add_argument("--benchmark", action="store_true")
    p_storage.add_argument("--partition", action="store_true")
    p_storage.add_argument("--postgres", action="store_true")
    p_storage.set_defaults(func=_run_storage)

    p_train = sub.add_parser("train", help="Train the model")
    p_train.set_defaults(func=_run_train)

    p_eval = sub.add_parser("evaluate", help="Evaluate the model")
    p_eval.set_defaults(func=_run_evaluate)

    p_all = sub.add_parser("all", help="Run the full pipeline end to end")
    p_all.set_defaults(func=_run_all)

    args = parser.parse_args()
    configure_logging(get_settings())
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())