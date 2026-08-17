# Azure ↔ GCP service mapping

Same architecture, different native services for each concern.

| Concern | Azure | GCP |
|---|---|---|
| Reasoning / planning engine | Azure OpenAI (GPT-4o) | Vertex AI (Gemini) |
| Network isolation | Private VNet | VPC Service Controls (VPC-SC) |
| Agent framework | Semantic Kernel | Agent Development Kit (ADK) / Vertex AI Agent Builder |
| Tool-calling protocol | MCP | MCP — same protocol, cloud-agnostic |
| Gold layer / lakehouse | Databricks / Synapse | BigQuery, with Dataform-managed views |
| Data governance & RBAC | Unity Catalog | Dataplex + policy tags, IAM |
| Identity pass-through | Entra ID pass-through auth | IAM with end-user credentials / authorized views |
| API gateway, rate limit, cost tracking | Azure API Management (APIM) | Apigee / Cloud Endpoints |
| Per-query cost guardrail | Warehouse-specific limits | BigQuery maximum bytes billed |
| Query validation before execution | Syntax check + self-correct | BigQuery dry-run + self-correct |
| Explainability | Return SQL + steps with answer | Return SQL + steps with answer |
