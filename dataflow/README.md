# GCP Dataflow — Architect-Level Interview Prep

A structured, architect-level knowledge base for **Google Cloud Dataflow** and
**Apache Beam**, written for interview preparation across the full
beginner → advanced/architect spectrum.

Each topic is a self-contained Markdown file with:
- Conceptual deep-dive (what + why, not just how)
- Architecture diagrams — both **Mermaid** (renders natively on GitHub) and
  **hand-sketched colorful PNGs** (whiteboard-style, in each topic's
  `diagrams/` folder)
- Code snippets (PySpark/Beam Python SDK primarily, with Java notes where it matters)
- Real production gotchas & tuning knobs
- Interview Q&A — beginner → advanced → architect/system-design level
- Common follow-up/curveball questions with model answers

## How this repo is organized

Content is delivered incrementally, two topics per batch, in this order:

| # | Topic | Status |
|---|-------|--------|
| 01 | [Dataflow Fundamentals & Beam Programming Model](01-fundamentals/README.md) | ✅ |
| 02 | [Windowing & Triggers](02-windowing-triggers/README.md) | ✅ |
| 03 | [Runners & Execution Model](03-runners-execution/README.md) | ✅ |
| 04 | [Pipeline Design Patterns](04-pipeline-design-patterns/README.md) | ✅ |
| 05 | [Streaming vs Batch Architecture](05-streaming-vs-batch/README.md) | ✅ |
| 06 | [Autoscaling & Performance Tuning](06-autoscaling-performance/README.md) | ✅ |
| 07 | Dataflow SQL & Templates | ⏳ |
| 08 | Security, IAM & Networking (VPC-SC) | ⏳ |
| 09 | Monitoring & Observability | ⏳ |
| 10 | Cost Optimization | ⏳ |
| 11 | Dataflow vs Alternatives (Spark/Dataproc/Flink) | ⏳ |
| 12 | CI/CD & Testing | ⏳ |
| 13 | Real-world Architecture Case Studies | ⏳ |
| 14 | Interview Q&A Bank (scenario-based, cross-topic) | ⏳ |

## Diagram sources

Every `diagrams/` folder contains the **Python scripts** (`sketch_*.py`,
using `assets/sketch_lib.py`, a matplotlib/xkcd-based sketch-diagram helper)
used to regenerate the hand-sketched PNGs, so diagrams stay editable and
reproducible — just run `python3 diagrams/sketch_<name>.py`.

Mermaid diagrams live inline inside each topic's `README.md` — GitHub, GitLab,
and most modern Markdown viewers render these automatically with no extra
tooling.

## Suggested study path

1. Read topics 01–02 (fundamentals) before anything else — every later topic
   assumes you're fluent in PCollections, PTransforms, and windowing.
2. Topics 03–07 build the "how it runs and how you write it" mental model.
3. Topics 08–10 are the operational/architect layer — this is where senior
   and staff-level interviews differ from mid-level ones.
4. Topic 11 sharpens your "why Dataflow and not X" narrative for
   architecture-round interviews.
5. Topics 12–14 are interview-day ammo: real scenarios, trade-off questions,
   and a cross-topic Q&A bank.
