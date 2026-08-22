# gcp-data-engineering

Data engineering on Google Cloud — architect-level reference notes,
production-ready pipeline examples, and interview prep, organized by topic.
This is a working knowledge base, not a single project: each top-level
folder is self-contained with its own README.

## What's here

- **[dataflow/](dataflow/)** — Architect-level knowledge base for
  Google Cloud Dataflow and Apache Beam. Topic-by-topic deep dives
  (fundamentals, windowing & triggers, runners, pipeline design patterns,
  streaming vs batch, autoscaling) with Mermaid + hand-sketched diagrams,
  code snippets, production gotchas, and interview Q&A at every level.

- **[pipelines/](pipelines/)** — Standalone, production-ready GCP pipeline
  examples: Cloud Functions to BigQuery/GCS/SFTP, a Beam ETL medallion
  (Bronze/Silver/Gold) pipeline, a serverless event-driven ingestion
  pipeline, and a unified BigQuery data export framework. Each includes
  setup docs, IAM/security notes, and tests.

- **[interview-prep/](interview-prep/)** — Answers, talking points, and
  diagrams for cloud and agentic AI interview questions, covering both
  Azure and GCP where relevant (agentic AI architecture, conversational
  analytics, etc.).

## Who this is for

Reference material for GCP data engineering work and interview
preparation — useful whether you're building a pipeline, studying for an
architect-level interview, or looking for a production pattern to adapt.

## How to use this repo

Start with whichever folder matches what you need:
- Building or learning Dataflow/Beam → [dataflow/](dataflow/)
- Copying a working pipeline pattern → [pipelines/](pipelines/)
- Prepping for an interview → [interview-prep/](interview-prep/)
