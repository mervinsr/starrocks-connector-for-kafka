"""One performance run through Kafka Connect: produce at a fixed rate and measure the connector and StarRocks.

Freshness lag is sampled every 0.5 s as now minus the newest Kafka timestamp visible in the table.
The table and the topic are dropped after the run so that later runs start from a quiet cluster.
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import threading
import time
import urllib.request

from common import (SR_DB, SR_FE, compaction_score, connector_config, create_topic, delete, delete_topic, deploy,
                    scalar, sql, start_producer, visible_version)

WORK = os.environ["WORK"]
# CPU of the FE and BE/CN processes is sampled over SSH when SR_SSH_USER and SR_CN (comma-separated BE/CN
# hosts) are set. SR_SSH_PASSWORD uses sshpass; without it, SSH must log in with a key.
SSH_USER = os.environ.get("SR_SSH_USER")
SSH_PASSWORD = os.environ.get("SR_SSH_PASSWORD")
CN_HOSTS = [h for h in os.environ.get("SR_CN", "").split(",") if h]


def proc_cpu_seconds(host, pattern):
    ssh = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR"]
    if SSH_PASSWORD:
        ssh = ["sshpass", "-p", SSH_PASSWORD] + ssh
    cmd = "pid=$(pgrep -f '%s' | head -1); awk '{print $14+$15}' /proc/$pid/stat; getconf CLK_TCK" % pattern
    out = subprocess.run(ssh + ["%s@%s" % (SSH_USER, host), cmd], capture_output=True, text=True,
                         timeout=30).stdout.split()
    return float(out[0]) / float(out[1])


def cpu_snapshot():
    if not SSH_USER or not CN_HOSTS:
        return {"fe": 0.0, "cn": 0.0}
    snapshot = {"fe": proc_cpu_seconds(SR_FE, "[c]om.starrocks.StarRocksFE")}
    snapshot["cn"] = sum(proc_cpu_seconds(h, "[s]tarrocks_be") for h in CN_HOSTS)
    return snapshot


def fe_counters():
    text = urllib.request.urlopen("http://%s:%s/metrics" % (SR_FE, os.environ.get("SR_HTTP_PORT", "8030")),
                                  timeout=10).read().decode()
    counters = {}
    for name in ("starrocks_fe_txn_begin", "starrocks_fe_txn_success", "starrocks_fe_txn_failed"):
        match = re.search(r"^%s\{[^}]*\} (\S+)$" % name, text, re.M)
        counters[name] = float(match.group(1)) if match else 0.0
    return counters


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100.0 * (len(values) - 1))))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--mode", choices=["merge", "default"], required=True)
    parser.add_argument("--tasks", type=int, required=True)
    parser.add_argument("--rate", type=int, required=True, help="rows per second, 0 for as fast as possible")
    parser.add_argument("--rows", type=int, help="rows to produce, default rate * duration")
    parser.add_argument("--duration", type=int, default=180)
    parser.add_argument("--flush", type=int)
    parser.add_argument("--window", type=int, default=1000)
    parser.add_argument("--pad", type=int, default=150)
    parser.add_argument("--partitions", type=int, default=32)
    parser.add_argument("--table-type", choices=["dup", "pk"], default="dup")
    parser.add_argument("--buckets", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--backlog", action="store_true", help="produce everything before starting the connector")
    parser.add_argument("--extra", default="{}", help="extra connector config as json")
    args = parser.parse_args()

    rows = args.rows or args.rate * args.duration
    table = "perf_" + args.name.replace("-", "_")
    topic = table
    sql("CREATE DATABASE IF NOT EXISTS %s" % SR_DB)
    sql("DROP TABLE IF EXISTS %s.%s FORCE" % (SR_DB, table))
    key = "DUPLICATE KEY(kafka_ts, id)" if args.table_type == "dup" else "PRIMARY KEY(id)"
    order = "" if args.table_type == "dup" else "ORDER BY(kafka_ts)"
    columns = "(kafka_ts BIGINT, id BIGINT, name VARCHAR(64), score INT, pad VARCHAR(512))" \
        if args.table_type == "dup" else "(id BIGINT, kafka_ts BIGINT, name VARCHAR(64), score INT, pad VARCHAR(512))"
    sql("CREATE TABLE %s.%s %s %s DISTRIBUTED BY HASH(id) BUCKETS %d %s"
        % (SR_DB, table, columns, key, args.buckets, order))
    delete(table)
    delete_topic(topic)
    time.sleep(2)
    create_topic(topic, args.partitions)

    extra = json.loads(args.extra)
    config = connector_config(topic, table, args.tasks, args.mode, args.flush, args.window, extra)
    producer = None
    if args.backlog:
        producer = start_producer(topic, 0, rows, 0, args.pad, "%s/logs/producer-%s.log" % (WORK, table))
        producer.wait()

    versions0 = visible_version(table)
    fe0 = fe_counters()
    cpu0 = cpu_snapshot()
    start = time.time()
    deploy(table, config)
    if not args.backlog:
        producer = start_producer(topic, 0, rows, args.rate, args.pad, "%s/logs/producer-%s.log" % (WORK, table))
    produce_start = time.time()

    lag_samples = []  # (seconds since start, lag ms)
    points = []  # every 10 s: rows loaded and the compaction score
    state = {"producer_end": None, "max_cs": 0.0}
    stop = threading.Event()

    # Rows and compaction score every 10 s in another thread, so that the lag is sampled every 0.5 s.
    def sample_slow():
        while not stop.is_set():
            now = time.time()
            max_cs, _ = compaction_score(table)
            state["max_cs"] = max(state["max_cs"], max_cs)
            loaded = scalar("SELECT COUNT(*) FROM %s.%s" % (SR_DB, table))
            points.append({"t": round(now - produce_start), "rows": loaded, "max_cs": max_cs,
                           "lag_ms": int(lag_samples[-1][1]) if lag_samples else None})
            print("t=%3ds rows=%d max_cs=%.1f lag_ms=%s" % (now - produce_start, loaded, max_cs,
                                                           points[-1]["lag_ms"]), flush=True)
            stop.wait(max(0.0, 10 - (time.time() - now)))

    slow = threading.Thread(target=sample_slow, daemon=True)
    slow.start()
    last_count = 0.0
    while True:
        now = time.time()
        if state["producer_end"] is None and producer.poll() is not None:
            state["producer_end"] = now
        newest = scalar("SELECT MAX(kafka_ts) FROM %s.%s WHERE kafka_ts > %d"
                        % (SR_DB, table, int(now * 1000) - 600000))
        if newest is not None and not args.backlog and state["producer_end"] is None:
            lag_samples.append((now - produce_start, now * 1000 - newest))
        # Once everything is produced, check every second whether everything is loaded.
        if state["producer_end"] is not None and now - last_count >= 1:
            last_count = now
            if scalar("SELECT COUNT(*) FROM %s.%s" % (SR_DB, table)) >= rows:
                break
        if now - produce_start > args.duration * 4 + 600:
            print("timeout", flush=True)
            break
        time.sleep(max(0.0, 0.5 - (time.time() - now)))
    end = time.time()
    stop.set()
    slow.join()
    producer_end = state["producer_end"]
    max_cs = state["max_cs"]
    lag_points = points
    loaded = scalar("SELECT COUNT(*) FROM %s.%s" % (SR_DB, table))
    versions1 = visible_version(table)
    fe1 = fe_counters()
    cpu1 = cpu_snapshot()
    elapsed = end - start

    steady = [lag for t, lag in lag_samples
              if t >= args.warmup and producer_end is not None and t <= producer_end - produce_start]
    result = {
        "name": args.name, "mode": args.mode, "tasks": args.tasks, "rate": args.rate, "rows": rows,
        "loaded": loaded, "table_type": args.table_type, "flush_ms": int(config["bufferflush.intervalms"]),
        "window_ms": args.window if args.mode == "merge" else None, "backlog": args.backlog,
        "elapsed_s": round(elapsed, 1),
        "produce_s": round((producer_end or end) - produce_start, 1),
        "drain_s": round(end - (producer_end or end), 1),
        "throughput_rows_s": round(loaded / max(1e-9, end - produce_start)),
        "lag_ms": {"p50": percentile(steady, 50), "p95": percentile(steady, 95), "p99": percentile(steady, 99),
                   "max": max(steady) if steady else None, "mean": statistics.mean(steady) if steady else None,
                   "samples": len(steady)},
        "transactions": versions1 - versions0,
        "transactions_per_s": round((versions1 - versions0) / elapsed, 2),
        "fe_txn_begin": fe1["starrocks_fe_txn_begin"] - fe0["starrocks_fe_txn_begin"],
        "fe_txn_failed": fe1["starrocks_fe_txn_failed"] - fe0["starrocks_fe_txn_failed"],
        "fe_cpu_cores": round((cpu1["fe"] - cpu0["fe"]) / elapsed, 2),
        "cn_cpu_cores_total": round((cpu1["cn"] - cpu0["cn"]) / elapsed, 2),
        "max_compaction_score": max_cs,
        "points": lag_points,
        "lag_series": [(round(t, 1), int(lag)) for t, lag in lag_samples],
        "extra": extra,
    }
    print(json.dumps({k: v for k, v in result.items() if k not in ("points", "lag_series")}), flush=True)
    with open(os.path.join(WORK, "perf-results.jsonl"), "a") as f:
        f.write(json.dumps(result) + "\n")
    delete(table)
    delete_topic(topic)
    sql("DROP TABLE IF EXISTS %s.%s FORCE" % (SR_DB, table))


if __name__ == "__main__":
    main()
