# Architecting enterprise-grade Agentic AI on GCP

To build an enterprise-grade Agentic AI system on Google Cloud, I would architect a solution that strictly separates the LLM's reasoning engine from the data execution environment — the same principle that applies on any cloud, just mapped onto GCP-native services. We cannot allow an LLM to generate unchecked SQL directly against our Gold layer. Here is my three-pillar approach.

![Three Pillars of Agentic AI Architecture](./diagrams/three-pillars.png)

## 1. Orchestration & the Model Context Protocol (MCP)

- **The AI engine:** Vertex AI with Gemini (e.g. Gemini 2.5 Pro) for the core reasoning and planning engine, deployed within a VPC Service Controls (VPC-SC) perimeter so data never leaves our organization's security boundary.
- **The agent framework:** Google's Agent Development Kit (ADK) or Vertex AI Agent Builder, or a custom agentic loop built around the Model Context Protocol (MCP) — the same open standard, regardless of cloud. Instead of writing custom integration code for every data source, we expose our BigQuery Gold layer and metadata as standardized MCP servers. Gemini acts as the MCP client, dynamically discovering which tools it has available (e.g. `query_sales_data`, `get_customer_churn_metric`) and calling them safely via JSON-RPC.

## 2. Secure data interaction (the execution layer)

![Request Flow Architecture](./diagrams/request-flow.png)

- **No raw SQL on Gold:** the agent never writes raw SQL directly against base tables. Instead, a semantic layer — curated, Dataform-managed views in our BigQuery Gold dataset — sits between the agent and the data.
- **BigQuery + Dataplex integration:** the tools exposed to the agent execute against BigQuery using authorized views and end-user credentials, so the query actually runs as the calling user — not a shared, privileged service account. Dataplex's data catalog supplies the schema and business definitions, fed into the agent's system prompt as a strict data dictionary to minimize hallucinations.
- **The validation loop:** BigQuery's dry-run capability validates query syntax and estimated bytes scanned before execution. If validation fails, the agent catches the error and self-corrects before showing anything to the user.

## 3. Governance, security, and guardrails

- **Identity & RBAC:** IAM combined with Dataplex policy tags enforces column- and row-level security. Because queries run with the end user's own credentials rather than a shared service account, if a marketing user asks the agent for HR salary data, BigQuery blocks the query at the data layer — the agent is bound by the user's actual access, not its own.
- **Cost & rate limiting:** Apigee (or Cloud Endpoints) sits in front of the Vertex AI endpoint to enforce rate limits, track cost per user, and automatically cut off runaway agent loops. BigQuery's custom quotas and maximum-bytes-billed setting cap the cost of any single query, so one bad agent-generated query can't scan the entire warehouse.
- **Explainability:** the UI never returns just a single number. The agent always returns its answer alongside the SQL query it generated or the steps it took, ensuring human-in-the-loop verification.

---

### Why this answer lands well

1. **Cutting-edge architecture (MCP):** shows engagement with the latest AI integration standards, and that the thinking isn't locked to one vendor's tooling.
2. **Solves the security problem the GCP-native way:** end-user credentials + Dataplex policy tags shows you understand how GCP actually enforces governance, not just that governance matters in the abstract.
3. **Manages operational risk:** Apigee for the API layer and BigQuery's own cost controls show you're thinking about both LLM cost and warehouse cost — agents can blow up either one.
