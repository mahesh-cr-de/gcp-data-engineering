# Cloud AI Interview Prep

A growing collection of my answers, talking points, and diagrams for cloud and agentic AI interview questions — covering both Azure and GCP. Used for interview prep and as source material for social posts and YouTube content.

## Structure

```
cloud-ai-interview-prep/
├── README.md
└── questions/
    └── NN-example-question/
        ├── question.md          # the question, cloud-agnostic
        ├── cloud-mapping.md     # Azure ↔ GCP service mapping, if answered for multiple clouds
        ├── azure/
        │   ├── full-answer.md
        │   ├── talking-points.md
        │   ├── post.md
        │   └── diagrams/
        │       ├── three-pillars.png
        │       └── request-flow.png
        └── gcp/
            ├── full-answer.md
            ├── talking-points.md
            ├── post.md
            └── diagrams/
                ├── three-pillars.png
                └── request-flow.png
```

The architecture is usually the same regardless of cloud — only the native services change. Each question folder holds one cloud-agnostic `question.md`, plus one subfolder per cloud with the full answer, condensed talking points, a post, and diagrams.

## Adding a new question

1. Create a new folder under `questions/`, numbered sequentially: `02-<short-slug>/`
2. Add a cloud-agnostic `question.md`
3. Add one subfolder per cloud you've answered it for (`azure/`, `gcp/`, `aws/`, ...) with `full-answer.md` at minimum
4. Add `talking-points.md` once you've condensed it for verbal delivery
5. Drop any diagrams in a `diagrams/` subfolder inside the relevant cloud folder
6. Add `post.md` if/when you turn it into a post
7. If the question spans multiple clouds, add a `cloud-mapping.md` at the question level

## Index

| # | Question | Clouds answered | Topics |
|---|----------|------------------|--------|
| 01 | [Architecting an enterprise-grade agentic AI system](questions/01-agentic-ai-architecture/question.md) | [GCP](questions/01-agentic-ai-architecture/gcp/full-answer.md) (+ [Azure mapping](questions/01-agentic-ai-architecture/cloud-mapping.md)) | MCP, semantic layer, identity pass-through, cost/rate control |
| 02 | [Enterprise Conversational Analytics](questions/02-enterprise-conversational-analytics/question.md) | [GCP](questions/02-enterprise-conversational-analytics/gcp/full-answer.md) | Vertex AI Agent, BigQuery, governance, cost control, MCP |
