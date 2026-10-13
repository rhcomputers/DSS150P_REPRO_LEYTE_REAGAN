# Format Benchmark Report

Sample size: 8,924 rows

| Format | Write (s) | Read (s) | Size (bytes) | Size (MB) |
|--------|-----------|----------|--------------|-----------|
| csv | 0.219 | 0.059 | 2,109,980 | 2.01 |
| jsonl | 0.125 | 0.308 | 9,784,450 | 9.33 |
| parquet | 0.051 | 0.082 | 673,725 | 0.64 |

## Decision

- **Parquet** is used for everything the pipeline writes (staging, curated, partitioned, model outputs). It has the smallest footprint and fastest reads.
- **CSV** is used only for source exports and interoperability with tools that cannot read Parquet.
- **JSONL** is used only for API-shaped outputs and manifests where line records help with streaming.
