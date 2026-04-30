# Engineering Notes: Lakehouse Processing Benchmark

## Why this project exists

This repository was designed as an engineering experiment to explore how different data transformation strategies behave within a lakehouse architecture.

Instead of assuming which tool or engine is “better”, the goal is to observe how each approach performs under controlled conditions, making architectural trade-offs explicit rather than implicit.

This is not a tutorial or a showcase of tools.
It is an attempt to model decision-making in real data platforms.

---

## Experiment philosophy

Most discussions around data processing engines are driven by:

* prior experience
* community bias
* or isolated benchmarks

This project takes a different approach.

It keeps ingestion, storage, and persistence constant, while varying only the transformation layer.
By doing so, it allows observing how each engine behaves as data volume increases, without introducing confounding variables.

The objective is not to reach a universal conclusion, but to understand system behavior under different constraints.

---

## System flow

```text
Synthetic ingestion → raw object storage → transformation engine → Trino/Iceberg persistence → validation + metrics
```

The pipeline is intentionally simple, so that the impact of the transformation layer becomes more visible.

---

## What is being measured

The benchmark focuses on end-to-end behavior rather than isolated execution time.

Key dimensions include:

* total runtime across the pipeline
* behavior under increasing data volumes
* rows requested vs rows successfully persisted
* timeout and failure scenarios
* operational overhead introduced by each engine

This allows evaluating not just performance, but also reliability and scalability characteristics.

---

## Design rationale

Several decisions were made to keep the experiment controlled and meaningful:

* **Synthetic data generation**
  Ensures reproducibility and avoids confidentiality constraints.

* **Separation between raw and final layers**
  Makes data ownership and processing boundaries explicit.

* **Transformer abstraction (factory + base classes)**
  Allows introducing new engines without changing ingestion or persistence logic.

* **Consistent persistence layer (Trino + Iceberg)**
  Ensures that differences in output are due to transformation strategy, not storage inconsistencies.

---

## Observing trade-offs

Each transformation approach introduces different characteristics:

* Simpler engines reduce overhead but are constrained by memory and single-node execution.
* Distributed engines introduce coordination and startup costs but offer horizontal scalability.
* The same pipeline structure can behave very differently depending on where complexity is introduced.

This project is designed to surface these differences, not to eliminate them.

---

## What this project demonstrates

More than comparing tools, this repository demonstrates:

* how to structure experiments in data engineering
* how to isolate variables in system design
* how architectural decisions impact end-to-end behavior
* how to reason about trade-offs beyond intuition

The codebase reflects these concerns through its modular structure and explicit boundaries.

---

## Intended audience

This project is structured for engineers who want to understand:

* how data systems behave under different processing strategies
* how to reason about engine selection in real-world pipelines
* how to make architectural decisions based on observation rather than assumption

It is particularly relevant for data engineers and platform engineers working with lakehouse architectures.
