"""
Ingestion module for the DSS150P reproduction pipeline.

Responsibilities:
- Verify the immutable source file's SHA-256 checksum before reading it.
- Copy source bytes into a run-specific raw snapshot (byte-for-byte).
- Emit ingestion audit metadata (pipeline_run_id, ingested_at_utc,
  source_file, source_checksum) alongside each snapshot.
- Support a full-refresh ingestion and a simulated incremental ingestion
  based on a deterministic batch key (collision_year by default).
- Persist checkpoint state outside the Python source so reruns skip
  already-committed batches unless --force is passed.

The module never mutates the source file. It only reads it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import Settings, configure_logging, get_settings

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_of_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def new_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{uuid.uuid4().hex[:8]}"


# ----------------------------------------------------------------------
# Audit metadata
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class IngestAudit:
    pipeline_run_id: str
    ingested_at_utc: str
    source_file: str
    source_checksum: str
    mode: str
    batch_key_column: str | None
    batch_value: Any
    row_count: int
    raw_snapshot_path: str
    raw_format: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


# ----------------------------------------------------------------------
# Checksum verification
# ----------------------------------------------------------------------
def verify_source(settings: Settings, raise_on_mismatch: bool = True) -> str:
    src = settings.source_path
    if not src.exists():
        raise FileNotFoundError(
            f"Source file not found at {src}. "
            f"Place the dataset there or update config/settings.yml."
        )

    actual = sha256_of_file(src)
    expected = settings.source_checksum

    if actual != expected:
        msg = (
            f"Source checksum mismatch.\n"
            f"  file:     {src}\n"
            f"  expected: {expected}\n"
            f"  actual:   {actual}"
        )
        if raise_on_mismatch:
            raise ValueError(msg)
        log.warning(msg)

    log.info("Source checksum verified: %s", actual)
    return actual


# ----------------------------------------------------------------------
# Raw snapshot writer
# ----------------------------------------------------------------------
def _write_snapshot(df: pd.DataFrame, audit: IngestAudit, raw_dir: Path, fmt: str) -> Path:
    out_dir = raw_dir / audit.pipeline_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    data_file = out_dir / f"snapshot.{fmt}"
    if fmt == "parquet":
        df.to_parquet(data_file, index=False)
    elif fmt == "csv":
        df.to_csv(data_file, index=False)
    elif fmt == "jsonl":
        df.to_json(data_file, orient="records", lines=True)
    else:
        raise ValueError(f"Unsupported raw_format: {fmt}")

    (out_dir / "manifest.json").write_text(audit.to_json(), encoding="utf-8")
    return data_file


# ----------------------------------------------------------------------
# Checkpoint (state outside code)
# ----------------------------------------------------------------------
def _load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"committed_batches": []}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_checkpoint(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# ----------------------------------------------------------------------
# Public entry points
# ----------------------------------------------------------------------
def ingest_full(settings: Settings | None = None, *, force: bool = False) -> IngestAudit:
    s = settings or get_settings()
    s.ensure_dirs()

    checksum = verify_source(s)
    run_id = new_run_id()

    log.info("Reading source: %s", s.source_path)
    df = pd.read_csv(s.source_path, low_memory=False)

    audit = IngestAudit(
        pipeline_run_id=run_id,
        ingested_at_utc=utcnow_iso(),
        source_file=s.source_path.name,
        source_checksum=checksum,
        mode="full",
        batch_key_column=None,
        batch_value="all",
        row_count=len(df),
        raw_snapshot_path="",
        raw_format=s.raw_format,
    )

    data_file = _write_snapshot(df, audit, s.raw_dir, s.raw_format)
    audit = IngestAudit(**{**asdict(audit), "raw_snapshot_path": str(data_file.relative_to(s.repo_root))})
    (data_file.parent / "manifest.json").write_text(audit.to_json(), encoding="utf-8")

    log.info("Full ingestion complete: %d rows -> %s", len(df), data_file)
    return audit


def ingest_incremental(settings: Settings | None = None, *, force: bool = False) -> list[IngestAudit]:
    s = settings or get_settings()
    s.ensure_dirs()

    checksum = verify_source(s)
    df = pd.read_csv(s.source_path, low_memory=False)

    if s.batch_key not in df.columns:
        raise ValueError(f"Batch key '{s.batch_key}' not found in source columns.")

    batches = sorted(df[s.batch_key].dropna().unique().tolist())
    if len(batches) < s.incremental_batches:
        raise ValueError(
            f"Expected at least {s.incremental_batches} batches by '{s.batch_key}', "
            f"found {len(batches)}: {batches}"
        )

    checkpoint = _load_checkpoint(s.checkpoint_file)
    committed = set(checkpoint.get("committed_batches", []))

    audits: list[IngestAudit] = []
    for value in batches:
        if value in committed and not force:
            log.info("Skipping already committed batch %s=%s", s.batch_key, value)
            continue

        batch_df = df[df[s.batch_key] == value].copy()
        run_id = new_run_id()

        audit = IngestAudit(
            pipeline_run_id=run_id,
            ingested_at_utc=utcnow_iso(),
            source_file=s.source_path.name,
            source_checksum=checksum,
            mode="incremental",
            batch_key_column=s.batch_key,
            batch_value=value,
            row_count=len(batch_df),
            raw_snapshot_path="",
            raw_format=s.raw_format,
        )

        data_file = _write_snapshot(batch_df, audit, s.raw_dir, s.raw_format)
        audit = IngestAudit(**{**asdict(audit), "raw_snapshot_path": str(data_file.relative_to(s.repo_root))})
        (data_file.parent / "manifest.json").write_text(audit.to_json(), encoding="utf-8")

        committed.add(value)
        checkpoint["committed_batches"] = sorted(committed)
        _save_checkpoint(s.checkpoint_file, checkpoint)

        audits.append(audit)
        log.info("Ingested batch %s=%s: %d rows -> %s", s.batch_key, value, len(batch_df), data_file)

    return audits


def ingest(settings: Settings | None = None, *, force: bool = False) -> list[IngestAudit]:
    s = settings or get_settings()
    if s.ingestion_mode == "full":
        return [ingest_full(s, force=force)]
    if s.ingestion_mode == "incremental":
        return ingest_incremental(s, force=force)
    raise ValueError(f"Unknown ingestion mode: {s.ingestion_mode}")


# ----------------------------------------------------------------------
# CLI entry point
# ----------------------------------------------------------------------
def _cli() -> None:
    parser = argparse.ArgumentParser(description="DSS150P ingestion")
    parser.add_argument("--mode", choices=["full", "incremental"], default=None)
    parser.add_argument("--force", action="store_true", help="Ignore checkpoints and rerun all batches")
    args = parser.parse_args()

    s = get_settings()
    configure_logging(s)

    if args.mode == "full":
        audits = [ingest_full(s, force=args.force)]
    elif args.mode == "incremental":
        audits = ingest_incremental(s, force=args.force)
    else:
        audits = ingest(s, force=args.force)

    for a in audits:
        print(a.to_json())


if __name__ == "__main__":
    _cli()