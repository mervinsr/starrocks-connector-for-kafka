"""Functional end-to-end tests through a real Kafka Connect cluster.

Each scenario produces rows with unique ids, lets the connector load them, and checks the table.
Scenario restart_rebalance restarts the tasks, adds a second worker, kills it with SIGKILL, and checks
that a primary key table still ends up with every row exactly once.
"""
import os
import subprocess
import sys
import time

from common import (HARNESS, SR_DB, connector_config, create_topic, delete, delete_topic, deploy, rest, scalar, sql,
                    start_producer, task_states, visible_version, wait_until)

WORK = os.environ["WORK"]
DUP_DDL = ("CREATE TABLE %s.%s (id BIGINT, name VARCHAR(64), score INT, pad VARCHAR(256), kafka_ts BIGINT) "
           "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8")
PK_DDL = ("CREATE TABLE %s.%s (id BIGINT, name VARCHAR(64), score INT, pad VARCHAR(256), kafka_ts BIGINT) "
          "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8")

results = []


def check(name, ok, detail):
    results.append((name, ok, detail))
    print("[%s] %s: %s" % ("PASS" if ok else "FAIL", name, detail), flush=True)


def expected_score(count):
    return sum(i % 100 for i in range(count))


def load_all(name, mode, table_ddl, rows, tasks=4):
    table = "%s_%s" % (name, mode)
    topic = "e2e_" + table
    sql(table_ddl % (SR_DB, table))
    create_topic(topic, 8)
    deploy(table, connector_config(topic, table, tasks, mode))
    version0 = visible_version(table)
    start = time.time()
    producer = start_producer(topic, 0, rows, 0, 32, "%s/logs/producer-%s.log" % (WORK, table))
    producer.wait()
    loaded = wait_until(lambda: scalar("SELECT COUNT(*) FROM %s.%s" % (SR_DB, table)) >= rows, 600)
    cost = time.time() - start
    stats = sql("SELECT COUNT(*), COUNT(DISTINCT id), SUM(score) FROM %s.%s" % (SR_DB, table))[1][0]
    versions = visible_version(table) - version0
    check("%s[%s]" % (name, mode), loaded and stats[0] == rows and stats[1] == rows
          and stats[2] == expected_score(rows),
          "rows %d, distinct %d, %.1fs, %d transactions" % (stats[0], stats[1], cost, versions))
    delete(table)
    delete_topic(topic)


def restart_rebalance(mode, rows=2_000_000, rate=50_000):
    table = "rebalance_%s" % mode
    topic = "e2e_" + table
    sql(PK_DDL % (SR_DB, table))
    create_topic(topic, 8)
    deploy(table, connector_config(topic, table, 4, mode))
    producer = start_producer(topic, 0, rows, rate, 32, "%s/logs/producer-%s.log" % (WORK, table))
    events = []
    time.sleep(10)
    rest("POST", "/connectors/%s/restart?includeTasks=true&onlyFailed=false" % table)
    events.append("restarted tasks")
    time.sleep(10)
    subprocess.run([HARNESS + "/kafka_up.sh", "2"], check=True, stdout=subprocess.DEVNULL)
    time.sleep(10)
    workers = sorted(set(w for _, _, w in task_states(table)))
    events.append("2 workers: %s" % workers)
    subprocess.run(["pkill", "-9", "-f", os.path.join(WORK, "connect-2.properties")])
    events.append("killed worker 2")
    producer.wait()
    loaded = wait_until(lambda: scalar("SELECT COUNT(*) FROM %s.%s" % (SR_DB, table)) >= rows, 600)
    stats = sql("SELECT COUNT(*), SUM(score) FROM %s.%s" % (SR_DB, table))[1][0]
    states = task_states(table)
    check("restart_rebalance[%s]" % mode, loaded and stats[0] == rows and stats[1] == expected_score(rows)
          and all(s == "RUNNING" for _, s, _ in states),
          "rows %d, %s, tasks %s" % (stats[0], "; ".join(events), states))
    delete(table)
    delete_topic(topic)


def main():
    sql("CREATE DATABASE IF NOT EXISTS %s" % SR_DB)
    scenarios = sys.argv[1:] or ["load_dup", "load_pk", "restart_rebalance"]
    for mode in ("merge", "default"):
        if "load_dup" in scenarios:
            load_all("dup", mode, DUP_DDL, 1_000_000)
        if "load_pk" in scenarios:
            load_all("pk", mode, PK_DDL, 1_000_000)
        if "restart_rebalance" in scenarios:
            restart_rebalance(mode)
    print("\nSUMMARY")
    for name, ok, detail in results:
        print("%-28s %s  %s" % (name, "PASS" if ok else "FAIL", detail))
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)


if __name__ == "__main__":
    main()
