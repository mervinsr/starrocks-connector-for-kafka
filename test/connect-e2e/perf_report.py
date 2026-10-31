"""Prints the performance results as markdown tables."""
import json
import os
import sys

path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.environ["WORK"], "perf-results.jsonl")
runs = [json.loads(line) for line in open(path) if line.strip()]


def seconds(ms):
    return "-" if ms is None else "%.2f" % (ms / 1000.0)


def config(r):
    if r["mode"] == "merge":
        return "merge commit, window %d ms, flush %d ms" % (r["window_ms"], r["flush_ms"])
    return "default, flush %d ms" % r["flush_ms"]


print("| Run | Table | Tasks | Config | Offered rows/s | Loaded rows/s | Freshness p50 / p95 / p99 (s) | "
      "Transactions/s | FE CPU (cores) | CN CPU (cores, 3 nodes) | Max compaction score | Drain (s) |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in runs:
    if r["backlog"]:
        continue
    lag = r["lag_ms"]
    print("| %s | %s | %d | %s | %d | %d | %s / %s / %s | %.2f | %.2f | %.2f | %.0f | %.1f |" % (
        r["name"], r["table_type"], r["tasks"], config(r), r["rate"], r["throughput_rows_s"],
        seconds(lag["p50"]), seconds(lag["p95"]), seconds(lag["p99"]), r["transactions_per_s"],
        r["fe_cpu_cores"], r["cn_cpu_cores_total"], r["max_compaction_score"], r["drain_s"]))

backlog = [r for r in runs if r["backlog"]]
if backlog:
    print()
    print("| Run | Tasks | Config | Rows | Time (s) | Rows/s | Transactions | FE CPU (cores) | CN CPU (cores) | "
          "Max compaction score |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in backlog:
        print("| %s | %d | %s | %d | %.1f | %d | %d | %.2f | %.2f | %.0f |" % (
            r["name"], r["tasks"], config(r), r["loaded"], r["elapsed_s"], r["throughput_rows_s"],
            r["transactions"], r["fe_cpu_cores"], r["cn_cpu_cores_total"], r["max_compaction_score"]))
