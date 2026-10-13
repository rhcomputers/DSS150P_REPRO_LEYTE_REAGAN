# DSS150P — Reproduction of a Predictive Analytics Project

**Author:** LEYTE, REAGAN
**Course:** DSS150P — Fundamentals of Data Engineering
**Activity:** Integrated Individual Laboratory Activity #4

---

## 1. What This Repository Does

This repository reproduces a predictive analytics solution as a reproducible, rerun-safe data engineering system. The predictive model is the downstream consumer of an engineered data product — the emphasis is on engineering the pipeline, not on maximizing accuracy.

**Task:** Binary classification of fatal road collisions.
**Target:** `is_fatal = (collision_severity == 1)` derived from the UK DfT STATS19 `collision_severity` field.
**Source:** UK Department for Transport (DfT) STATS19 road collision data, 2021–2025, 513,801 rows, 44 columns.

For context on why this project has a predictive layer despite the original group project being a harmonization effort, see [docs/scope_note.md](docs/scope_note.md).

---

## 2. Reproducing This Project on a Clean Machine

Follow these steps in order. No Python source code should need to be edited.

### 2.1 Prerequisites

- Python 3.11
- Git
- Docker Desktop (with Docker Compose v2)
- PowerShell (Windows) or bash (macOS/Linux)

### 2.2 Clone the repository

```bash
git clone https://github.com/rhcomputers/DSS150P_REPRO_LEYTE_REAGAN.git
cd DSS150P_REPRO_LEYTE_REAGAN