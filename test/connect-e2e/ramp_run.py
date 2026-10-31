"""Steps up the load on one connector and measures each step, to find the highest rate it sustains.

Several producers follow the same schedule of rates without a gap between the steps. For every step the run
reports the produced and loaded rows/s, the freshness (now minus the newest Kafka timestamp visible in the
table) after the first 30 s of the step, the backlog at the end of the step, the transactions, the compaction
score and the CPU. It stops early once the connector falls far behind.
"""
import argparse
import json
import os
import subprocess
import threading
import time

from common import (BROKER, KAFKA_HOME, SR_DB, compaction_score, connector_config, create_topic, delete,
                    delete_topic, deploy, scalar, sql, visible_version)
from perf_run import cpu_snapshot, fe_counters, percentile

WORK = os.environ["WORK"]
HARNESS = os.path.dirname(os.path.abspath(__file__))
CLASSES = os.path.join(WORK, "producer-classes")


def compile_producer():
    os.makedirs(CLASSES, exist_ok=True)
    subprocess.run(["javac", "-cp", KAFKA_HOME + "/libs/*", "-d", CLASSES, os.path.join(HARNESS, "StepProducer.java")],
                   check=True)


def start_producers(topic, schedule, count, pad, name, keys=0, delete_every=0):
    """Starts the producers; producer k sends ids from k * 10^12, or updates keys from there (see StepProducer)."""
    producers = []
    for k in range(count):
        share = ",".join("%d:%d" % (rate // count, seconds) for rate, seconds in schedule)
        progress = os.path.join(WORK, "progress-%s-%d" % (name, k))
        if os.path.exists(progress):
            os.remove(progress)
        log = open(os.path.join(WORK, "logs", "producer-%s-%d.log" % (name, k)), "w")
        args = [BROKER, topic, str(k * 10 ** 12), share, str(pad), progress]
        if keys:
            args += [str(keys), str(delete_every)]
        producers.append((progress, subprocess.Popen(
            ["java", "-Xmx2g", "-cp", CLASSES + ":" + KAFKA_HOME + "/libs/*", "StepProducer"] + args,
            stdout=log, stderr=subprocess.STDOUT)))
    return producers


def produced_by_each(producers):
    """The rows each producer has sent, in the order of the producers."""
    counts = []
    for progress, _ in producers:
        try:
            counts.append(int(open(progress).read().split()[1]))
        except (OSError, IndexError, ValueError):
            counts.append(0)
    return counts


def produced(producers):
    total = 0
    for progress, _ in producers:
        try:
            total += int(open(progress).read().split()[1])
        except (OSError, IndexError, ValueError):
            pass
    return total


def local_cpu_seconds(pattern):
    """CPU seconds used so far by the local processes whose command line contains the pattern."""
    total = 0.0
    ticks = os.sysconf("SC_CLK_TCK")
    for pid in subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True).stdout.split():
        try:
            fields = open("/proc/%s/stat" % pid).read().rsplit(")", 1)[1].split()
            total += (int(fields[11]) + int(fields[12])) / ticks
        except (OSError, IndexError, ValueError):
            pass
    return total


def client_cpu():
    return {"connect": local_cpu_seconds(os.path.join(WORK, "connect-")),
            "broker": local_cpu_seconds(os.path.join(WORK, "server.properties")),
            "producers": local_cpu_seconds("StepProducer")}


def newest_kafka_ts(table, now_ms):
    # A narrow range keeps the query cheap on a large table. Fall back to a wide one when the data is late.
    for window_ms in (15000, 600000):
        newest = scalar("SELECT MAX(kafka_ts) FROM %s.%s WHERE kafka_ts > %d" % (SR_DB, table, now_ms - window_ms))
        if newest is not None:
            return newest
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--mode", choices=["merge", "default"], required=True)
    parser.add_argument("--tasks", type=int, required=True)
    parser.add_argument("--rates", required=True, help="comma-separated rows/s of the steps")
    parser.add_argument("--step-seconds", type=int, default=90)
    parser.add_argument("--settle-seconds", type=int, default=30, help="ignored at the start of each step")
    parser.add_argument("--producers", type=int, default=3)
    parser.add_argument("--flush", type=int)
    parser.add_argument("--window", type=int, default=1000)
    parser.add_argument("--pad", type=int, default=150)
    parser.add_argument("--partitions", type=int, default=48)
    parser.add_argument("--buckets", type=int, default=16)
    parser.add_argument("--max-backlog-seconds", type=float, default=60,
                        help="stop when the backlog exceeds this many seconds of the current rate")
    parser.add_argument("--extra", default="{}", help="extra connector config as json")
    args = parser.parse_args()

    rates = [int(r) for r in args.rates.split(",")]
    schedule = [(rate, args.step_seconds) for rate in rates]
    table = topic = "ramp_" + args.name.replace("-", "_")
    compile_producer()
    sql("CREATE DATABASE IF NOT EXISTS %s" % SR_DB)
    sql("DROP TABLE IF EXISTS %s.%s FORCE" % (SR_DB, table))
    sql("CREATE TABLE %s.%s (kafka_ts BIGINT, id BIGINT, name VARCHAR(64), score INT, pad VARCHAR(512)) "
        "DUPLICATE KEY(kafka_ts, id) DISTRIBUTED BY HASH(id) BUCKETS %d" % (SR_DB, table, args.buckets))
    delete(table)
    delete_topic(topic)
    time.sleep(3)
    # Keep only a few minutes of data in Kafka, the connector reads it within seconds.
    create_topic(topic, args.partitions, {"retention.ms": 300000, "segment.bytes": 134217728})
    config = connector_config(topic, table, args.tasks, args.mode, args.flush, args.window, json.loads(args.extra))
    deploy(table, config)

    producers = start_producers(topic, schedule, args.producers, args.pad, table)
    start = time.time()
    lag_samples = []
    stop = threading.Event()

    def sample_lag():
        while not stop.is_set():
            now = time.time()
            newest = newest_kafka_ts(table, int(now * 1000))
            lag_samples.append((now - start, None if newest is None else now * 1000 - newest))
            stop.wait(max(0.0, 0.5 - (time.time() - now)))

    sampler = threading.Thread(target=sample_lag, daemon=True)
    sampler.start()

    results = []
    step_start = start
    snapshot = {"versions": visible_version(table), "fe": fe_counters(), "cpu": cpu_snapshot(),
                "client": client_cpu(), "loaded": 0, "produced": 0}
    saturated = None
    for index, (rate, seconds) in enumerate(schedule):
        step_end = step_start + seconds
        max_cs = 0.0
        while time.time() < step_end:
            max_cs = max(max_cs, compaction_score(table)[0])
            time.sleep(min(10, max(0.0, step_end - time.time())))
        loaded = scalar("SELECT COUNT(*) FROM %s.%s" % (SR_DB, table))
        produced_rows = produced(producers)
        now = time.time()
        current = {"versions": visible_version(table), "fe": fe_counters(), "cpu": cpu_snapshot(),
                   "client": client_cpu(), "loaded": loaded, "produced": produced_rows}
        elapsed = now - step_start
        lags = [lag for t, lag in lag_samples if step_start - start + args.settle_seconds <= t <= now - start]
        missing = sum(1 for lag in lags if lag is None)
        lags = [lag for lag in lags if lag is not None]
        backlog = produced_rows - loaded
        step = {
            "step": index, "target_rows_s": rate,
            "produced_rows_s": round((produced_rows - snapshot["produced"]) / elapsed),
            "loaded_rows_s": round((loaded - snapshot["loaded"]) / elapsed),
            "backlog_rows": backlog,
            "backlog_s": round(backlog / max(1, rate), 1),
            "lag_ms": {"p50": percentile(lags, 50), "p95": percentile(lags, 95), "p99": percentile(lags, 99),
                       "max": max(lags) if lags else None, "samples": len(lags), "missing": missing},
            "transactions_per_s": round((current["versions"] - snapshot["versions"]) / elapsed, 2),
            "fe_txn_begin_per_s": round((current["fe"]["starrocks_fe_txn_begin"]
                                         - snapshot["fe"]["starrocks_fe_txn_begin"]) / elapsed, 2),
            "fe_cpu_cores": round((current["cpu"]["fe"] - snapshot["cpu"]["fe"]) / elapsed, 2),
            "cn_cpu_cores_total": round((current["cpu"]["cn"] - snapshot["cpu"]["cn"]) / elapsed, 2),
            "max_compaction_score": max_cs,
            "client_cpu_cores": {k: round((current["client"][k] - snapshot["client"][k]) / elapsed, 2)
                                 for k in current["client"]},
        }
        results.append(step)
        print(json.dumps(step), flush=True)
        snapshot = current
        step_start = now
        if backlog > args.max_backlog_seconds * rate:
            saturated = index
            print("saturated at step %d: backlog %.0f s" % (index, backlog / rate), flush=True)
            break

    stop.set()
    sampler.join()
    for _, process in producers:
        process.kill()
    summary = {"name": args.name, "mode": args.mode, "tasks": args.tasks, "flush_ms": int(config["bufferflush.intervalms"]),
               "window_ms": args.window if args.mode == "merge" else None, "producers": args.producers,
               "partitions": args.partitions, "step_seconds": args.step_seconds, "saturated_step": saturated,
               "extra": json.loads(args.extra), "steps": results}
    with open(os.path.join(WORK, "ramp-results.jsonl"), "a") as f:
        f.write(json.dumps(summary) + "\n")
    delete(table)
    delete_topic(topic)
    sql("DROP TABLE IF EXISTS %s.%s FORCE" % (SR_DB, table))


if __name__ == "__main__":
    main()
