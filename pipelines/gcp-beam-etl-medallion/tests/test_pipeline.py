import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.window import FixedWindows
import datetime

logging.basicConfig(level=logging.INFO)

class ParseJson(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            # Basic schema enforcement
            if 'timestamp' not in record or 'id' not in record:
                raise ValueError("Missing required fields")
            record['processed_at'] = datetime.datetime.utcnow().isoformat()
            yield record
        except Exception as e:
            logging.error(f"Parse error: {e}")
            yield beam.pvalue.TaggedOutput('deadletter', element)  # Dead letter

class CleanTransform(beam.DoFn):
    def process(self, record):
        try:
            # Cleaning logic (PySpark-like)
            if 'amount' in record:
                record['amount'] = float(record['amount'])
            if 'category' in record:
                record['category'] = record['category'].strip().upper()
            # Dedup logic via composite key later
            yield record
        except Exception as e:
            logging.error(f"Transform error: {e}")
            yield beam.pvalue.TaggedOutput('deadletter', record)

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from GCS (supports JSON lines or CSV)
        raw = (p
               | 'ReadFromGCS' >> beam.io.ReadFromText('gs://your-bucket/input/*.jsonl')
               | 'Parse' >> beam.ParDo(ParseJson()).with_outputs('deadletter', main='main'))

        cleaned = (raw.main
                   | 'Window' >> beam.WindowInto(FixedWindows(3600))  # For incremental readiness
                   | 'Clean' >> beam.ParDo(CleanTransform()).with_outputs('deadletter', main='main'))

        # Write Bronze (raw-ish)
        (cleaned.main
         | 'WriteBronze' >> WriteToBigQuery(
             table='your-project:dataset.bronze_sales',
             schema='id:STRING, timestamp:TIMESTAMP, amount:FLOAT, category:STRING, processed_at:TIMESTAMP',
             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=BigQueryDisposition.WRITE_APPEND,  # Incremental append
             additional_bq_parameters={'timePartitioning': {'type': 'DAY', 'field': 'timestamp'}}
         ))

        # Dead letter queue
        (raw.deadletter
         | 'WriteDeadletter' >> beam.io.WriteToText('gs://your-bucket/errors/deadletter'))

        # Silver/Gold: In production, trigger Dataform SQL jobs or additional Beam transforms
        # Example aggregation in Beam for Gold
        gold = (cleaned.main
                | 'Aggregate' >> beam.GroupByKey()  # Or use SqlTransform for complex
                | 'ComputeMetrics' >> beam.Map(lambda x: {'category': x[0], 'total_sales': sum(r['amount'] for r in x[1])}))

        (gold
         | 'WriteGold' >> WriteToBigQuery(
             table='your-project:dataset.gold_sales_summary',
             schema='category:STRING, total_sales:FLOAT',
             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=BigQueryDisposition.WRITE_APPEND
         ))

if __name__ == '__main__':
    run()