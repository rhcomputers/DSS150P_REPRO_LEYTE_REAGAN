"""
Central configuration loader for the DSS150P reproduction pipeline.

Responsibilities:
- Locate the repository root regardless of where the process is invoked.
- Load config/settings.yml (non-secret configuration).
- Load environment variables from .env (secrets and machine-specific values).
- Expose a single immutable Settings object to every other module.

No other module should read YAML or environment variables directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


# ----------------------------------------------------------------------
# Path resolution
# ----------------------------------------------------------------------
def _find_repo_root(start: Path | None = None) -> Path:
    """
    Walk upwards from this file until we find the repo root.
    The repo root is the directory that contains both config/ and src/.
    This works no matter which working directory the caller used.
    """
    here = (start or Path(__file__)).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "config" / "settings.yml").exists() and (candidate / "src").exists():
            return candidate
    raise RuntimeError(
        "Could not locate repository root. Expected to find config/settings.yml "
        "and src/ in an ancestor directory of src/config.py."
    )


REPO_ROOT: Path = _find_repo_root()


# ----------------------------------------------------------------------
# Settings dataclass
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    """Immutable view of the pipeline configuration."""

    repo_root: Path
    raw_yaml: dict[str, Any]

    # --- project ---
    project_name: str
    project_version: str
    project_owner: str

    # --- resolved paths (absolute) ---
    source_dir: Path
    raw_dir: Path
    staging_dir: Path
    curated_dir: Path
    partitioned_dir: Path
    quarantine_dir: Path
    outputs_dir: Path
    model_dir: Path
    metadata_dir: Path
    state_dir: Path
    log_dir: Path

    # --- source ---
    source_filename: str
    source_checksum: str
    source_row_count: int
    source_column_count: int

    # --- ingestion ---
    ingestion_mode: str
    incremental_batches: int
    checkpoint_file: Path
    row_hash_columns: list[str]
    batch_key: str

    # --- storage ---
    raw_format: str
    staging_format: str
    curated_format: str
    enable_postgres: bool
    postgres_schema: str
    postgres_tables: dict[str, str]

    # --- model ---
    target_column: str
    target_derivation: str
    positive_class: int
    task: str
    algorithm: str
    random_seed: int
    test_size: float
    hyperparameters: dict[str, Any]

    # --- evaluation ---
    tolerance: dict[str, float]
    metrics_file: Path

    # --- logging ---
    log_level: str
    log_format: str

    # --- env secrets (may be None outside Docker) ---
    env: dict[str, str] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------
    @property
    def source_path(self) -> Path:
        return self.source_dir / self.source_filename

    def ensure_dirs(self) -> None:
        """Create every directory the pipeline writes to."""
        for p in [
            self.raw_dir,
            self.staging_dir,
            self.curated_dir,
            self.partitioned_dir,
            self.quarantine_dir,
            self.outputs_dir,
            self.model_dir,
            self.state_dir,
            self.log_dir,
        ]:
            p.mkdir(parents=True, exist_ok=True)


# ----------------------------------------------------------------------
# Loader
# ----------------------------------------------------------------------
def _resolve(root: Path, value: str) -> Path:
    """Turn a relative path from settings.yml into an absolute one."""
    p = Path(value)
    return p if p.is_absolute() else (root / p)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once and cache the result."""
    load_dotenv(REPO_ROOT / ".env", override=False)

    with open(REPO_ROOT / "config" / "settings.yml", "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    project = cfg["project"]
    paths = cfg["paths"]
    source = cfg["source"]
    ingestion = cfg["ingestion"]
    storage = cfg["storage"]
    model = cfg["model"]
    evaluation = cfg["evaluation"]
    logging_cfg = cfg["logging"]

    env = {k: v for k, v in os.environ.items() if k.startswith(("POSTGRES_", "AIRFLOW_", "PROJECT_", "ENV"))}

    return Settings(
        repo_root=REPO_ROOT,
        raw_yaml=cfg,
        project_name=project["name"],
        project_version=str(project["version"]),
        project_owner=project["owner"],
        source_dir=_resolve(REPO_ROOT, paths["source_dir"]),
        raw_dir=_resolve(REPO_ROOT, paths["raw_dir"]),
        staging_dir=_resolve(REPO_ROOT, paths["staging_dir"]),
        curated_dir=_resolve(REPO_ROOT, paths["curated_dir"]),
        partitioned_dir=_resolve(REPO_ROOT, paths["partitioned_dir"]),
        quarantine_dir=_resolve(REPO_ROOT, paths["quarantine_dir"]),
        outputs_dir=_resolve(REPO_ROOT, paths["outputs_dir"]),
        model_dir=_resolve(REPO_ROOT, paths["model_dir"]),
        metadata_dir=_resolve(REPO_ROOT, paths["metadata_dir"]),
        state_dir=_resolve(REPO_ROOT, paths["state_dir"]),
        log_dir=_resolve(REPO_ROOT, logging_cfg["log_dir"]),
        source_filename=source["filename"],
        source_checksum=source["checksum"],
        source_row_count=int(source["row_count"]),
        source_column_count=int(source["column_count"]),
        ingestion_mode=ingestion["mode"],
        incremental_batches=int(ingestion["incremental_batches"]),
        checkpoint_file=_resolve(REPO_ROOT, ingestion["checkpoint_file"]),
        row_hash_columns=list(ingestion["row_hash_columns"]),
        batch_key=ingestion["batch_key"],
        raw_format=storage["raw_format"],
        staging_format=storage["staging_format"],
        curated_format=storage["curated_format"],
        enable_postgres=bool(storage["enable_postgres"]),
        postgres_schema=storage["postgres_schema"],
        postgres_tables=dict(storage["postgres_tables"]),
        target_column=model["target_column"],
        target_derivation=model["target_derivation"],
        positive_class=int(model["positive_class"]),
        task=model["task"],
        algorithm=model["algorithm"],
        random_seed=int(model["random_seed"]),
        test_size=float(model["test_size"]),
        hyperparameters=dict(model["hyperparameters"]),
        tolerance=dict(evaluation["tolerance"]),
        metrics_file=_resolve(REPO_ROOT, evaluation["metrics_file"]),
        log_level=logging_cfg["level"],
        log_format=logging_cfg["format"],
        env=env,
    )


# ----------------------------------------------------------------------
# Logging helper
# ----------------------------------------------------------------------
def configure_logging(settings: Settings | None = None) -> None:
    """Configure root logging once for the whole pipeline."""
    import logging

    s = settings or get_settings()
    s.log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, s.log_level.upper(), logging.INFO),
        format=s.log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(s.log_dir / "pipeline.log", encoding="utf-8"),
        ],
    )


if __name__ == "__main__":
    import json

    s = get_settings()
    print(json.dumps(
        {
            "repo_root": str(s.repo_root),
            "source_path": str(s.source_path),
            "source_checksum": s.source_checksum,
            "target_column": s.target_column,
            "algorithm": s.algorithm,
            "random_seed": s.random_seed,
            "env_keys": sorted(s.env.keys()),
        },
        indent=2,
    ))