import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "assets"))
from sketch_lib import sketch_context, new_figure, box, arrow, title, note, save

OUT = os.path.join(os.path.dirname(__file__), "autoscaling-knobs.png")

with sketch_context():
    fig, ax = new_figure(14, 9.5)
    title(ax, "Dataflow Autoscaling — Signals & Control Knobs", y=9.2)

    # Signals feeding the decision
    box(ax, 0.4, 7.3, 3.2, 1.2, "Batch signal:\nCPU utilization +\nestimated remaining work", color="blue", fontsize=9.5)
    box(ax, 0.4, 5.8, 3.2, 1.2, "Streaming signal:\nbacklog (subscription lag)\n+ per-stage throughput", color="orange", fontsize=9.5)

    box(ax, 4.4, 6.6, 3.4, 1.4, "Autoscaling\nDecision Loop\n(~1 min cadence)", color="purple", fontsize=10.5)
    arrow(ax, 3.6, 7.9, 4.4, 7.3)
    arrow(ax, 3.6, 6.4, 4.4, 7.0)

    box(ax, 8.6, 6.6, 4.8, 1.4, "Add/remove Compute Engine\nworkers within [min, max] bounds", color="green", fontsize=9.5)
    arrow(ax, 7.8, 7.3, 8.6, 7.3)

    # Knobs row
    knobs = [
        (0.4, "--max_num_workers\nCaps scale-out\n(cost ceiling)", "teal"),
        (3.7, "--num_workers /\n--min_num_workers\nStarting / floor size", "pink"),
        (7.0, "--worker_machine_type\nVertical sizing\n(CPU/mem per worker)", "yellow"),
        (10.3, "Streaming Engine on/off\nShifts state off workers,\nenables leaner autoscale", "red"),
    ]
    for x, label, c in knobs:
        box(ax, x, 3.9, 3.1, 1.5, label, color=c, fontsize=9)

    note(ax, 0.4, 2.6,
         "Batch jobs also support Horizontal Autoscaling (worker count) — vertical sizing\n"
         "(machine type) is set once at submission and does NOT change mid-job.",
         fontsize=10.5)
    note(ax, 0.4, 1.7,
         "Right-fitting workers matters: too few vCPUs per worker starves memory-heavy DoFns\n"
         "(e.g. ML inference); too many wastes spend if the job is I/O- not CPU-bound.",
         fontsize=10.5)

    save(fig, OUT)
