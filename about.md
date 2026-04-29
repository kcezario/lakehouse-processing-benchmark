# Lakehouse Processing Benchmark: Project Notes

## Purpose

This project is a portfolio-grade engineering experiment to evaluate transformation engine choices in a lakehouse pipeline.

It compares the same data flow across three engines:

- Python
- Pandas
- Spark

The goal is to expose architecture trade-offs, not to build a tutorial script.

## End-to-End Flow

```text
Synthetic ingestion -> raw object storage -> transformation engine -> Trino/Iceberg persistence -> validation + metrics
```

## What Is Being Measured

- End-to-end runtime
- Engine runtime behavior by data volume
- Rows requested vs rows persisted
- Timeout and failure behavior
- Operational overhead vs scalability potential

## Why This Design

- **Synthetic data generation** keeps the repository safe for public use.
- **Raw layer + final table separation** makes ownership boundaries explicit.
- **Transformer factory pattern** allows adding engines with minimal coupling.
- **Trino as a persistence and validation layer** keeps write/read verification consistent.

## Key Trade-offs

- Python and Pandas are efficient for small-to-medium volumes but scale vertically.
- Spark introduces startup and infrastructure overhead, but scales for larger workloads.
- A single architecture can host all engines if contracts remain stable.

## Intended Audience

The repository is structured so a recruiter or senior engineer can quickly see:

- system decomposition,
- decision rationale,
- and measurable benchmark outcomes.