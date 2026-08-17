import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "assets"))
from sketch_lib import sketch_context, new_figure, box, arrow, title, note, save

OUT = os.path.join(os.path.dirname(__file__), "lambda-vs-kappa-architecture.png")

with sketch_context():
    fig, ax = new_figure(14, 10.6)
    title(ax, "Lambda vs Kappa Architecture (Dataflow's Unified Model Favors Kappa)", y=10.3)

    # Lambda
    ax.text(0.4, 9.3, "Lambda Architecture (two separate codepaths)", fontsize=12.5, fontweight="bold", color="#E53935")
    box(ax, 0.4, 7.9, 2.6, 1.0, "Raw Data\n(source)", color="blue", fontsize=9.5)
    box(ax, 3.4, 8.5, 3.0, 0.9, "Batch Layer\n(daily/hourly full recompute)", color="green", fontsize=9)
    box(ax, 3.4, 7.3, 3.0, 0.9, "Speed Layer\n(streaming approx. results)", color="orange", fontsize=9)
    box(ax, 6.8, 7.9, 3.0, 1.0, "Serving Layer\n(merge batch + speed views)", color="purple", fontsize=9)
    arrow(ax, 3.0, 8.4, 3.4, 8.95)
    arrow(ax, 3.0, 8.4, 3.4, 7.75)
    arrow(ax, 6.4, 8.95, 6.8, 8.5)
    arrow(ax, 6.4, 7.75, 6.8, 8.3)
    note(ax, 10.1, 8.4, "Two codebases to\nmaintain & keep in\nsync — real\noperational cost", fontsize=9, ha="left")

    # Kappa
    ax.text(0.4, 6.0, "Kappa Architecture (single streaming codepath — Beam's sweet spot)", fontsize=12.5, fontweight="bold", color="#43A047")
    box(ax, 0.4, 4.6, 2.6, 1.0, "Raw Data\n(Pub/Sub, replayable log)", color="blue", fontsize=9.5)
    box(ax, 3.4, 4.6, 4.0, 1.0, "ONE Beam pipeline\n(streaming; also runs in batch\nmode for backfill/replay)", color="teal", fontsize=9)
    box(ax, 8.0, 4.6, 3.0, 1.0, "Serving Layer\n(single source of truth)", color="purple", fontsize=9)
    arrow(ax, 3.0, 5.1, 3.4, 5.1)
    arrow(ax, 7.4, 5.1, 8.0, 5.1)

    note(ax, 0.4, 3.3,
         "Beam's unified model means the SAME pipeline code can reprocess historical data (bounded\n"
         "read of the same Pub/Sub-backed log / GCS archive) for backfills — no separate batch\n"
         "codebase required, unlike classic Lambda.",
         fontsize=10.5)
    note(ax, 0.4, 2.3,
         "Trade-off to name in interviews: Kappa needs a replayable, retained log (long Pub/Sub\n"
         "retention or a durable topic) to support reprocessing/backfill — that's a real cost/design\n"
         "constraint, not a free lunch.",
         fontsize=10.5)

    save(fig, OUT)
