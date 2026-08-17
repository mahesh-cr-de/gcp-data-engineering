import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "assets"))
from sketch_lib import sketch_context, new_figure, box, arrow, title, note, save

OUT = os.path.join(os.path.dirname(__file__), "batch-vs-streaming.png")

with sketch_context():
    fig, ax = new_figure(14, 9.5)
    title(ax, "Batch vs Streaming — Same Model, Different Trade-offs", y=9.2)

    # Batch column
    box(ax, 0.5, 7.6, 6.0, 1.0, "BATCH\nBounded PCollection (finite, known size)", color="blue", fontsize=11)
    rows_b = [
        "Source: GCS files, BigQuery table export",
        "Runs once, processes all data, then terminates",
        "Autoscaling driven by CPU utilization + remaining work estimate",
        "Latency: minutes-hours (throughput-optimized)",
        "Cost: pay only for job duration, workers torn down after",
        "Failure recovery: rerun the whole job (usually cheap, bounded)",
    ]
    y = 7.0
    for r in rows_b:
        box(ax, 0.5, y, 6.0, 0.65, r, color="green", fontsize=8.7, alpha=0.75)
        y -= 0.78

    # Streaming column
    box(ax, 7.3, 7.6, 6.2, 1.0, "STREAMING\nUnbounded PCollection (infinite, continuous)", color="orange", fontsize=11)
    rows_s = [
        "Source: Pub/Sub, Kafka, unbounded reads",
        "Runs continuously; never terminates on its own",
        "Autoscaling driven by backlog + throughput signals",
        "Latency: seconds (latency-optimized, windowing needed)",
        "Cost: pay continuously while job runs, 24/7",
        "Failure recovery: checkpointed state resumes from last snapshot",
    ]
    y = 7.0
    for r in rows_s:
        box(ax, 7.3, y, 6.2, 0.65, r, color="pink", fontsize=8.7, alpha=0.75)
        y -= 0.78

    note(ax, 0.5, 1.6,
         "Unified model: the SAME Beam transforms (ParDo, GroupByKey, Combine) work on both —\n"
         "windowing is what makes aggregation over an infinite stream well-defined (Topic 02).",
         fontsize=10.5)
    note(ax, 0.5, 0.8,
         "Architect framing: choose streaming when the business genuinely needs sub-minute\n"
         "freshness; otherwise batch is simpler, cheaper, and easier to reason about and debug.",
         fontsize=10.5)

    save(fig, OUT)
