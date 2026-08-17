# GCP Data Engineering Examples

A collection of practical data engineering examples and sample code for Google Cloud Platform services. Each example is designed as a standalone, production-ready reference implementation with clear documentation, best practices, and security considerations.

## 📚 Examples

### ✅ Completed

- **[Unified Data Export Framework](./unified-data-export-framework/)**
  - Cloud Scheduler to Cloud Functions Gen2 using Python 3.12
  - BigQuery Storage API batch streaming to CSV, TSV, or pipe-delimited files
  - Optional gzip, GCS and verified-host SFTP delivery, audit logs, and idempotency
  - Secret Manager credentials, structured Cloud Logging, retries, and pytest tests

- **[Cloud Function: BigQuery to GCS to SFTP](./cloud-function-bq-to-gcs-sftp/)**
  - Server-side BigQuery EXTRACT to GCS (pipe-delimited CSV)
  - GCS file relay to SFTP destination
  - Secret Manager for secure configuration
  - Optimized for large datasets without memory constraints

- **[Beam ETL Medallion on GCP](./gcp-beam-etl-medallion/)**
  - Apache Beam pipeline for Bronze/Silver/Gold medallion architecture
  - Sample JSONL input data with duplicate records for testing
  - Dataflow-ready pipeline structure with unit tests
  - Includes a detailed architecture guide and example pipeline implementation

- **[Serverless Event-Driven API Pipeline](./serverless-api-event-pipeline/)**
  - Cloud Scheduler to Cloud Function Gen2 API ingestion
  - Pub/Sub direct Cloud Storage subscription for an immutable Bronze layer
  - BigQuery staging and incremental Silver `MERGE`
  - Fully serverless alternative for lightweight ingestion workloads

### 🔄 Planned (Coming Daily)

- [ ] **Dataflow** - Batch and stream processing pipelines
- [ ] **Dataform** - SQL-based data transformation and orchestration
- [ ] **Pub/Sub** - Real-time messaging and event processing
- [ ] **Cloud Composer** - Apache Airflow-based workflow orchestration
- [ ] **VertexAI** - ML model training and deployment

## 📋 Project Structure

```
gcp-data-engineering-examples/
├── cloud-function-bq-to-gcs-sftp/
│   ├── cloud_function.md          # Setup and deployment guide
│   ├── main.py                    # Cloud Function code
│   ├── requirements.txt           # Python dependencies
│   └── README.md                  # Example-specific documentation
├── gcp-beam-etl-medallion/
│   ├── gcp-beam-etl-medallion.md  # Detailed architecture guide
│   ├── requirements.txt           # Python dependencies
│   ├── data/
│   │   └── sample.jsonl           # Sample input data with duplicates
│   ├── pipeline/
│   │   ├── __init__.py
│   │   └── main.py                # Core Beam pipeline implementation
│   └── tests/
│       └── test_pipeline.py       # Basic unit tests
├── README.md                       # This file
└── ...                             # Future examples
```

## 🚀 Quick Start

Each example includes its own documentation and setup instructions. Start with any example that interests you:

```bash
cd <example-directory>
cat README.md  # or cloud_function.md for setup details
```

## 💡 Key Features Across Examples

- **Security First**: Credentials managed via Secret Manager (never hardcoded)
- **Scalability**: Designed for production workloads
- **Clear Documentation**: Step-by-step setup and deployment guides
- **IAM Best Practices**: Least-privilege service accounts and roles
- **Cost Optimization**: Efficient use of GCP resources

## 🛠️ Prerequisites

- GCP project with billing enabled
- `gcloud` CLI installed and configured
- Appropriate GCP APIs enabled (varies by example)

## 📖 Documentation

Each example includes detailed documentation covering:
- Architecture overview
- Prerequisites and setup
- Deployment instructions
- IAM configuration
- Usage examples
- Troubleshooting

## 🤝 Contributing

Feel free to extend these examples or create variations for your specific use case.

## 📝 License

These examples are provided as-is for reference and learning purposes.

---

**Last Updated**: June 2026  
**Status**: Work in Progress (adding daily examples)
