## Question

How would you architect an enterprise-grade Agentic AI system that lets business users query enterprise data using natural language — without letting the LLM run unchecked queries against production data?

**Topics:** LLM orchestration, Model Context Protocol (MCP), semantic layer, data governance, identity pass-through, cost/rate control

**Role context:** Engineering Manager (Data)

**Cloud variants:** [Azure](azure/full-answer.md) · [GCP](gcp/full-answer.md)

The underlying architecture is the same on every cloud — separate the LLM's reasoning from the data execution layer, never let it touch raw data directly, and enforce identity and cost controls at the platform level rather than trusting the model. Only the specific services change. See `cloud-mapping.md` for the full service-by-service mapping.
