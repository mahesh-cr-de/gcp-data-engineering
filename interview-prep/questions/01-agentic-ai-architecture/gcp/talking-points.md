# Talking points — GCP version, condensed for verbal delivery

**Opening line** (frames everything else):

> "I'd separate the LLM's reasoning from the data execution environment — the model plans and asks, it never touches data directly. The services are GCP-native, but the principle is the same on any cloud."

## Mnemonic: Think → Touch → Track

### 1. Think (orchestration)
- Vertex AI Gemini inside a VPC Service Controls perimeter — data never leaves the org boundary
- MCP is still the term to drop: expose the BigQuery Gold layer as standardized MCP servers
- Agent (ADK / Agent Builder) discovers tools dynamically and calls them via JSON-RPC

### 2. Touch (secure execution)
- "Never raw SQL against Gold" — say it explicitly
- Curated, Dataform-managed views, not direct table access
- Dataplex catalog supplies the data dictionary fed into the system prompt
- BigQuery dry-run validates the query before execution; agent self-corrects on failure

### 3. Track (governance)
- End-user credentials / authorized views — the query runs AS the user, so IAM + Dataplex policy tags enforce column/row security at the data layer itself
- Apigee in front of Vertex AI — rate limits, per-user cost tracking, kills runaway loops
- BigQuery's maximum-bytes-billed setting caps the cost of any single query
- Explainability — SQL/steps always returned alongside the answer

**Closing line:**

> "The services change from cloud to cloud — Vertex AI, BigQuery, Dataplex, Apigee here — but the goal doesn't: safe and auditable by construction, not because we're trusting the model to behave."

## Delivery tip
If the interviewer is GCP-shop, lead with this version directly. If they're cloud-agnostic or evaluating breadth, mention up front that you've architected the equivalent on Azure too (Azure OpenAI / Unity Catalog / APIM) — it signals you're not locked into one vendor's mental model.

## Likely follow-ups to prep for
- How does BigQuery's INVOKER / authorized-view model actually pass through end-user identity?
- How would you version or test changes to the Dataform-managed semantic views?
- What's the failure mode if Dataplex policy tags and the MCP server's own auth disagree?
- How would this change if you needed write-back actions instead of just reads?
