import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "assets"))
from sketch_lib import sketch_context, new_figure, box, arrow, title, note, save

OUT = os.path.join(os.path.dirname(__file__), "performance-tuning-levers.png")

with sketch_context():
    fig, ax = new_figure(14, 12.2)
    title(ax, "Performance Tuning Levers — Where the Bottleneck Usually Is", y=11.7)

    levers = [
        (0.4, 9.5, "Hot keys / data skew\nFix: key-salting, better\nkey choice, CombineFn", "red"),
        (4.9, 9.5, "Fusion limiting parallelism\nFix: Reshuffle() to force\na fusion break", "orange"),
        (9.4, 9.5, "GroupByKey instead of\nCombine\nFix: use CombinePerKey\n(combiner lifting)", "yellow"),
        (0.4, 7.4, "Undersized workers\n(CPU/memory-bound DoFn,\ne.g. ML inference)\nFix: bigger machine_type,\nor batch external calls", "teal"),
        (4.9, 7.4, "Small side input\nrepeatedly rebuilt\nFix: cache in DoFn.setup(),\nnot per-element", "purple"),
        (9.4, 7.4, "Too many small\nexternal I/O calls\nFix: GroupIntoBatches\nbefore calling external API", "pink"),
    ]
    for x, y, label, c in levers:
        box(ax, x, y, 4.1, 1.7, label, color=c, fontsize=9)

    box(ax, 2.6, 5.6, 8.8, 1.3,
        "Diagnosis workflow: Dataflow job graph UI → find the stage with high wall time /\n"
        "low parallelism → check System Lag (streaming) or elapsed time per stage (batch) →\n"
        "match symptom to lever above.",
        color="blue", fontsize=10)

    note(ax, 0.4, 4.4,
         "Rule of thumb: fix the SINGLE slowest/most backlogged stage first — in a fused\n"
         "pipeline, downstream stages often just look slow because they're starved upstream.",
         fontsize=10.5)
    note(ax, 0.4, 3.5,
         "Always profile before tuning — guessing at performance fixes on a distributed system\n"
         "wastes engineering time and can make things worse (e.g. over-parallelizing a cheap stage).",
         fontsize=10.5)

    save(fig, OUT)
