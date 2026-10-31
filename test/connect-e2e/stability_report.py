"""Markdown tables of the stability runs in $WORK, recomputed from the time series that stability_run.py saves.

For every run: the freshness before the first fault, how long and how badly the freshness was disrupted after it,
how long nothing was loaded at all, the task failures, and whether every row arrived, with the duplicates.
A sample counts as disrupted when the freshness is above twice the baseline p99, and at least 2 s above it.
"""
import glob
import json
import os
import sys

from perf_run import percentile

WORK = os.environ["WORK"]


def disruption(freshness, series, faults, settle, produce_end):
    first = faults[0] if faults else None
    samples = [(t, lag) for t, lag, _ in freshness if settle <= t <= produce_end]
    base = [lag for t, lag in samples if lag is not None and t < (first if first is not None else produce_end)]
    base_p50, base_p99 = percentile(base, 50), percentile(base, 99)
    result = {"base_p50_s": base_p50 / 1000 if base_p50 else None, "base_p99_s": base_p99 / 1000 if base_p99 else None}
    if first is None:
        return result
    threshold = max(2 * (base_p99 or 0), (base_p99 or 0) + 2000)
    after = [(t, lag) for t, lag in samples if t >= first]
    lags = [lag for _, lag in after if lag is not None]
    disrupted = longest = 0.0
    unknown = 0
    run_start = None
    for (t, lag), (t_next, _) in zip(after, after[1:] + [(produce_end, None)]):
        if lag is None:
            unknown += 1
        bad = lag is None or lag > threshold
        if bad:
            disrupted += t_next - t
            run_start = t if run_start is None else run_start
            longest = max(longest, t_next - run_start)
        else:
            run_start = None
    stall = 0.0
    points = [p for p in series if p.get("loaded") is not None]
    base_rates = [(b["loaded"] - a["loaded"]) / (b["t"] - a["t"]) for a, b in zip(points, points[1:])
                  if settle <= b["t"] < first]
    base_rate = sum(base_rates) / len(base_rates) if base_rates else 0
    for a, b in zip(points, points[1:]):
        if first <= a["t"] and b["t"] <= produce_end and b["loaded"] - a["loaded"] <= 0.05 * base_rate * (b["t"] - a["t"]):
            stall += b["t"] - a["t"]
    result.update({"max_lag_s": max(lags) / 1000 if lags else None, "disrupted_s": round(disrupted),
                   "longest_disruption_s": round(longest), "unknown_samples": unknown, "stalled_s": round(stall)})
    return result


def main():
    rows = {}
    path = os.path.join(WORK, "stability-results.jsonl")
    for line in open(path):
        r = json.loads(line)
        rows[r["name"]] = r
    names = sys.argv[1:] or list(rows)
    print("| run | mode | faults | base p50/p99 | max lag | longest disruption | disrupted | no rows loaded | "
          "tasks failed / restarts | missing | duplicates | drained |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    pk_runs = []
    for name in names:
        r = rows[name]
        if r.get("keys"):
            pk_runs.append(name)
            continue
        try:
            ts = json.load(open(os.path.join(WORK, "stability-%s.json" % name)))
        except OSError:
            continue
        events = ts["events"]
        produce_end = next((t for t, text in events if text == "producers done"), events[-1][0])
        faults = sorted(float(f.split(":", 1)[0]) for f in r["faults"])
        d = disruption(ts["freshness"], ts["series"], faults, 60, produce_end)
        data = r["data"]
        fmt = lambda v, unit="s": "-" if v is None else ("%.1f %s" % (v, unit))
        print("| %s | %s | %s | %s / %s | %s | %s | %s | %s | %d / %d | %d | %d (%.2f%%) | %s |" % (
            name, r["mode"], ", ".join(f.split(":", 1)[1] for f in r["faults"]) or "-",
            fmt(d["base_p50_s"]), fmt(d["base_p99_s"]), fmt(d.get("max_lag_s")),
            fmt(d.get("longest_disruption_s")), fmt(d.get("disrupted_s")), fmt(d.get("stalled_s")),
            r["tasks_failed"], r["task_restarts"], data["missing"], data["duplicates"],
            100.0 * data["duplicates"] / max(1, data["sent"]),
            "yes, %.0f s" % data["drain_s"] if data["drained"] else "no"))
    if pk_runs:
        print_pk(rows, pk_runs)


def print_pk(rows, names):
    """The primary key runs: whether every key ends at its last version, now and 60 s later."""
    print()
    print("| run | mode | faults | merge_condition | deletes | max lag | tasks failed / restarts | keys | "
          "consistent | stale | resurrected | missing | after 60 s | drained |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name in names:
        r, d = rows[name], rows[name]["data"]
        later = d.get("after_60s", {})
        lag = (r.get("lag_after_fault_ms") or {}).get("max")
        print("| %s | %s | %s | %s | %s | %s | %d / %d | %d | %s | %d | %d | %d | %s | %s |" % (
            name, r["mode"], ", ".join(f.split(":", 1)[1] for f in r["faults"]) or "-",
            "yes" if "sink.properties.merge_condition" in r["extra"] or r.get("merge_condition") else "no",
            "every %d" % r["delete_every"] if r.get("delete_every") else "no",
            "-" if lag is None else "%.1f s" % (lag / 1000), r["tasks_failed"], r["task_restarts"],
            d["expected_present"], "yes" if d["consistent"] else "**no**", d["stale"], d["resurrected"], d["missing"],
            "-" if not later else ("same" if later.get("consistent") == d["consistent"] and later.get("stale") == d["stale"]
                                   and later.get("missing") == d["missing"] else "changed: %s" % later),
            "yes, %.0f s" % d["drain_s"] if d["drained"] else "no"))


if __name__ == "__main__":
    main()
