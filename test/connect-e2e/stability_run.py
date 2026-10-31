"""Runs one connector under a steady load while faults are injected, and measures the impact and the recovery.

Producers send rows with unique ids following --schedule. Each --fault runs an action at a given second after the
start, for example "180:cn_kill:<host>" and "240:cn_start:<host>". Every second the run samples the freshness of
the tables, that is now minus the newest Kafka timestamp visible in a table, and keeps the worst table. Every 5 s
it samples the loaded rows, the task states, the compaction score and the alive StarRocks nodes. A task that stays
FAILED for --restart-failed-after seconds is restarted, as an operator or a watchdog would do. When the producers
are done, the run waits until the connector has loaded every row or --drain-timeout passes, then checks that
every produced id is in the tables and counts the duplicates.

Fault actions:
  cn_kill:<host>, cn_stop:<host>, cn_start:<host>        kill -9, stop or start the CN of a host
  fe_kill:<host|leader|follower>, fe_start:<host|last>   kill -9 or start an FE
  worker_kill:<n>, worker_start:<n>                      SIGKILL or start Connect worker n
  net_delay:<host>:<ms>, net_loss:<host>:<percent>, net_clear:<host>
                                                         netem on the default interface of a host, needs sudo
  compaction_off, compaction_on                          stop or resume the compaction of the tables
  fe_config:<key>=<value>                                ADMIN SET FRONTEND CONFIG on every alive FE
  bad_rows:<count>                                       rows that violate NOT NULL, into the first topic
  pause, resume                                          pause or resume the connector

The StarRocks hosts are reached over SSH as SR_SSH_USER (SR_SSH_PASSWORD uses sshpass), with StarRocks deployed
under SR_HOME (/home/disk1/sr). SR_CN lists the CN hosts, which the run brings back up after every run.
"""
import argparse
import json
import os
import re
import subprocess
import threading
import time

from common import (BROKER, HARNESS, KAFKA_HOME, SR_DB, SR_FES, connector_config, consumer_lag, create_topic, delete,
                    delete_topic, deploy, rest, sql)
from perf_run import percentile
from ramp_run import client_cpu, compile_producer, produced, produced_by_each, start_producers

WORK = os.environ["WORK"]
SSH_USER = os.environ.get("SR_SSH_USER")
SSH_PASSWORD = os.environ.get("SR_SSH_PASSWORD")
SR_HOME = os.environ.get("SR_HOME", "/home/disk1/sr")
CN_HOSTS = [h for h in os.environ.get("SR_CN", "").split(",") if h]
CN_PROCESS = "[s]tarrocks_be"
FE_PROCESS = "[c]om.starrocks.StarRocksFE"
# A login shell, since the start scripts need the JAVA_HOME of the login profile.
START_CN = "bash -lc 'cd %s/be && ./bin/start_cn.sh --daemon'" % SR_HOME
START_FE = "bash -lc 'cd %s/fe && ./bin/start_fe.sh --daemon'" % SR_HOME
# Ids of the rows that violate NOT NULL, above the ids of the producers.
BAD_ID_BASE = 9 * 10 ** 15


def ssh(host, command, timeout=180):
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
           "-o", "ConnectTimeout=10", "%s@%s" % (SSH_USER, host), command]
    if SSH_PASSWORD:
        cmd = ["sshpass", "-p", SSH_PASSWORD] + cmd
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode, (result.stdout + result.stderr).strip()
    except subprocess.TimeoutExpired:
        return -1, "timeout"


def brief(result):
    rc, out = result
    return "rc=%d %s" % (rc, " ".join(out.split())[-150:])


def frontends():
    columns, rows = sql("SHOW FRONTENDS", timeout=10)
    ip, role, alive = columns.index("IP"), columns.index("Role"), columns.index("Alive")
    return [(row[ip], row[role], row[alive] == "true") for row in rows]


def compute_nodes():
    columns, rows = sql("SHOW COMPUTE NODES", timeout=10)
    ip, alive = columns.index("IP"), columns.index("Alive")
    return [(row[ip], row[alive] == "true") for row in rows]


def set_fe_config(key, value):
    """Sets a mutable FE config on every alive FE, since a follower can become the leader."""
    done = []
    for host, _, alive in frontends():
        if alive:
            sql('ADMIN SET FRONTEND CONFIG ("%s" = "%s")' % (key, value), host=host, timeout=30)
            done.append(host)
    return "%s=%s on %s" % (key, value, ",".join(done))


def compaction_disable_key():
    _, rows = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_disable%'", timeout=10)
    names = [row[0] for row in rows]
    return "lake_compaction_disable_ids" if "lake_compaction_disable_ids" in names else names[0]


def newest_kafka_ts(table, now_ms):
    # A narrow range keeps the query cheap on a large table. Fall back to a wide one when the data is late.
    for window_ms in (15000, 600000):
        _, rows = sql("SELECT MAX(kafka_ts) FROM %s.%s WHERE kafka_ts > %d" % (SR_DB, table, now_ms - window_ms),
                      timeout=10)
        if rows[0][0] is not None:
            return rows[0][0]
    return None


def ensure_cluster_healthy(timeout_s=600):
    """Starts any CN or FE that is down and waits until all of them are alive again."""
    for host in CN_HOSTS:
        ssh(host, "pgrep -f '%s' >/dev/null || %s" % (CN_PROCESS, START_CN))
    for host in SR_FES:
        ssh(host, "pgrep -f '%s' >/dev/null || %s" % (FE_PROCESS, START_FE))
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            fes, cns = frontends(), compute_nodes()
            if all(f[2] for f in fes) and all(c[1] for c in cns) and any(f[1] == "LEADER" for f in fes):
                return True
        except Exception:  # noqa: BLE001, an FE may still be starting
            pass
        time.sleep(5)
    return False


class Run:
    def __init__(self, args, name, tables, topics):
        self.args = args
        self.name = name
        self.tables = tables
        self.topics = topics
        self.start = None
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.freshness = []
        self.series = []
        self.events = []
        self.producers = []
        self.killed_fe = []
        self.failed_since = {}
        self.restarts = 0
        self.failed_tasks = set()
        self.bad_sent = 0
        self.used = set()
        self.slow = []

    def now(self):
        return round(time.time() - self.start, 1)

    def event(self, text):
        t = self.now()
        with self.lock:
            self.events.append((t, text))
        print("[%7.1fs] %s" % (t, text), flush=True)

    def table_ids(self):
        _, rows = sql("SELECT TABLE_ID FROM information_schema.tables_config WHERE TABLE_SCHEMA = '%s' AND "
                      "TABLE_NAME IN (%s)" % (SR_DB, ",".join("'%s'" % t for t in self.tables)), timeout=10)
        return [row[0] for row in rows]

    def pick_fe(self, which):
        fes = frontends()
        if which == "leader":
            return next(f[0] for f in fes if f[1] == "LEADER")
        if which == "follower":
            return next(f[0] for f in fes if f[1] != "LEADER" and f[2])
        return which

    def act(self, action):
        kind, _, arg = action.partition(":")
        self.used.add(kind)
        if kind in ("cn_kill", "cn_stop", "cn_start"):
            command = {"cn_kill": "pkill -9 -f '%s'" % CN_PROCESS,
                       "cn_stop": "bash -lc 'cd %s/be && ./bin/stop_cn.sh'" % SR_HOME,
                       "cn_start": START_CN}[kind]
            return "%s %s" % (arg, brief(ssh(arg, command)))
        if kind == "fe_kill":
            host = self.pick_fe(arg)
            self.killed_fe.append(host)
            return "%s %s" % (host, brief(ssh(host, "pkill -9 -f '%s'" % FE_PROCESS)))
        if kind == "fe_start":
            host = self.killed_fe[-1] if arg in ("", "last") else arg
            return "%s %s" % (host, brief(ssh(host, START_FE)))
        if kind == "worker_kill":
            return subprocess.run(["pkill", "-9", "-f", os.path.join(WORK, "connect-%s.properties" % arg)]).returncode
        if kind == "worker_start":
            result = subprocess.run([os.path.join(HARNESS, "kafka_up.sh"), arg], capture_output=True, text=True)
            return result.stdout.strip()[-200:]
        if kind in ("net_delay", "net_loss", "net_clear"):
            host, _, value = arg.partition(":")
            dev = "$(/usr/sbin/ip route show default | awk '{print $5; exit}')"
            if kind == "net_clear":
                command = "sudo -n /usr/sbin/tc qdisc del dev %s root" % dev
            else:
                netem = ("delay %sms" % value) if kind == "net_delay" else ("loss %s%%" % value)
                command = "sudo -n /usr/sbin/tc qdisc replace dev %s root netem %s" % (dev, netem)
            return "%s %s" % (host, brief(ssh(host, command)))
        if kind == "compaction_off":
            return set_fe_config(compaction_disable_key(), ";".join(str(i) for i in self.table_ids()))
        if kind == "compaction_on":
            return set_fe_config(compaction_disable_key(), "")
        if kind == "fe_config":
            key, _, value = arg.partition("=")
            return set_fe_config(key, value)
        if kind == "bad_rows":
            count = int(arg)
            rows = "".join('{"id":%d,"name":"bad","score":null,"pad":"x"}\n' % (BAD_ID_BASE + self.bad_sent + i)
                           for i in range(count))
            self.bad_sent += count
            result = subprocess.run([KAFKA_HOME + "/bin/kafka-console-producer.sh", "--bootstrap-server", BROKER,
                                     "--topic", self.topics[0]], input=rows, capture_output=True, text=True,
                                    timeout=120)
            return "rc=%d" % result.returncode
        if kind in ("pause", "resume"):
            rest("PUT", "/connectors/%s/%s" % (self.name, kind))
            return "ok"
        raise ValueError("unknown action " + action)

    def inject(self, faults):
        for at, action in faults:
            if self.stop.wait(max(0.0, self.start + at - time.time())):
                return
            try:
                self.event("fault %s: %s" % (action, self.act(action)))
            except Exception as e:  # noqa: BLE001, report and go on with the run
                self.event("fault %s failed: %s" % (action, str(e)[:300]))

    def sample_freshness(self):
        while not self.stop.is_set():
            began = time.time()
            worst, missing = None, 0
            try:
                for table in self.tables:
                    newest = newest_kafka_ts(table, int(began * 1000))
                    if newest is None:
                        missing += 1
                    else:
                        worst = max(worst or 0, began * 1000 - newest)
            except Exception:  # noqa: BLE001, StarRocks may be down: the sample is unknown
                worst, missing = None, -1
            with self.lock:
                self.freshness.append((round(began - self.start, 1), worst, missing))
            self.stop.wait(max(0.0, 1.0 - (time.time() - began)))

    def track_tasks(self):
        status = rest("GET", "/connectors/%s/status" % self.name)
        states = {}
        now = time.time()
        for task in status.get("tasks", []):
            state, task_id = task["state"], task["id"]
            states[state] = states.get(state, 0) + 1
            if state == "FAILED":
                if task_id not in self.failed_since:
                    self.failed_since[task_id] = now
                    self.failed_tasks.add(task_id)
                    trace = (task.get("trace") or "").strip().split("\n")
                    cause = next((line for line in reversed(trace) if line.startswith("Caused by")), trace[0])
                    self.event("task %d FAILED: %s" % (task_id, cause[:300]))
            else:
                self.failed_since.pop(task_id, None)
        limit = self.args.restart_failed_after
        if limit is not None and self.failed_since and now - min(self.failed_since.values()) >= limit:
            rest("POST", "/connectors/%s/restart?includeTasks=true&onlyFailed=true" % self.name)
            self.restarts += 1
            self.event("restarted the failed tasks %s" % sorted(self.failed_since))
            self.failed_since.clear()
        return states

    def sample_series(self):
        while not self.stop.is_set():
            began = time.time()
            point = {"t": round(began - self.start, 1), "produced": produced(self.producers)}
            try:
                point["loaded"] = sum(sql("SELECT COUNT(*) FROM %s.%s" % (SR_DB, t), timeout=30)[1][0][0]
                                      for t in self.tables)
            except Exception:  # noqa: BLE001
                point["loaded"] = None
            try:
                point["tasks"] = self.track_tasks()
            except Exception as e:  # noqa: BLE001, the workers may be down
                point["tasks"] = {"error": str(e)[:100]}
            try:
                _, rows = sql("SELECT MAX(MAX_CS), SUM(VISIBLE_VERSION) FROM information_schema.partitions_meta "
                              "WHERE DB_NAME = '%s' AND TABLE_NAME IN (%s)"
                              % (SR_DB, ",".join("'%s'" % t for t in self.tables)), timeout=10)
                point["cs"], point["versions"] = float(rows[0][0] or 0), int(rows[0][1] or 0)
            except Exception:  # noqa: BLE001
                point["cs"] = point["versions"] = None
            try:
                fes, cns = frontends(), compute_nodes()
                point["alive"] = {"fe": sum(1 for f in fes if f[2]), "cn": sum(1 for c in cns if c[1]),
                                  "leader": next((f[0] for f in fes if f[1] == "LEADER"), None)}
            except Exception:  # noqa: BLE001
                point["alive"] = None
            point["client_cpu"] = client_cpu()
            with self.lock:
                self.series.append(point)
            self.stop.wait(max(0.0, 5.0 - (time.time() - began)))


    def sample_slow(self):
        """Every 30 s: the CPU seconds of every FE and CN process, and the heap the Connect workers use."""
        hosts = [("fe", h) for h in SR_FES] + [("cn", h) for h in CN_HOSTS] if SSH_USER else []
        while not self.stop.is_set():
            began = time.time()
            point = {"t": round(began - self.start, 1), "cpu": {}, "heap_mb": {}}
            for role, host in hosts:
                rc, out = ssh(host, "pid=$(pgrep -f '%s' | head -1); [ -n \"$pid\" ] && awk '{print $14+$15}' "
                                    "/proc/$pid/stat && getconf CLK_TCK" % (FE_PROCESS if role == "fe" else CN_PROCESS),
                              timeout=20)
                parts = out.split()
                if rc == 0 and len(parts) == 2:
                    point["cpu"]["%s %s" % (role, host)] = float(parts[0]) / float(parts[1])
            for n in (1, 2, 3):
                pids = subprocess.run(["pgrep", "-f", os.path.join(WORK, "connect-%d.properties" % n)],
                                      capture_output=True, text=True).stdout.split()
                if pids:
                    out = subprocess.run(["jstat", "-gc", pids[0]], capture_output=True, text=True).stdout.split("\n")
                    if len(out) >= 2:
                        gc = dict(zip(out[0].split(), out[1].split()))
                        used = sum(float(gc.get(k, 0)) for k in ("S0U", "S1U", "EU", "OU"))
                        point["heap_mb"][n] = round(used / 1024)
            with self.lock:
                self.slow.append(point)
            self.stop.wait(max(0.0, 30.0 - (time.time() - began)))


def cpu_cores(slow, start_t, end_t):
    """Average cores of the FEs and of the CNs between two times, skipping processes that restarted."""
    points = [p for p in slow if start_t <= p["t"] <= end_t]
    cores = {"fe": 0.0, "cn": 0.0}
    for a, b in zip(points, points[1:]):
        for key, value in b["cpu"].items():
            if key in a["cpu"] and value >= a["cpu"][key]:
                cores[key.split()[0]] += (value - a["cpu"][key]) / (b["t"] - a["t"]) / max(1, len(points) - 1)
    return {k: round(v, 2) for k, v in cores.items()} if len(points) > 1 else None


def count_congruent(start, stop, offset, modulus, residue):
    """How many j in [start, stop) have (offset + j) % modulus == residue."""
    first = start + (residue - offset - start) % modulus
    return 0 if first >= stop else (stop - 1 - first) // modulus + 1


def pk_expected_present(sent, keys, delete_every):
    """How many keys of a producer exist at the end: the keys whose last version is not a delete.

    Row n updates key n % keys to version n / keys, so with sent - 1 = q * keys + r - 1, the first r keys end at
    version q and the others at q - 1. Version v of key j is a delete when (v + j) % delete_every is the last residue.
    """
    if sent <= 0:
        return 0
    if sent < keys:
        ends = [(0, sent, 0)]
    else:
        q, r = (sent - 1) // keys, (sent - 1) % keys + 1
        ends = [(0, r, q), (r, keys, q - 1)]
    present = 0
    for start, stop, version in ends:
        present += stop - start
        if delete_every:
            present -= count_congruent(start, stop, version, delete_every, delete_every - 1)
    return present


def pk_check(tables, sent_by_producer, keys, delete_every):
    """Compares a primary key table with the last version of every key that the producers sent."""
    totals = dict.fromkeys(("expected_present", "present", "correct", "stale", "resurrected", "ahead", "torn"), 0)
    for k, sent in enumerate(sent_by_producer):
        base = k * 10 ** 12
        totals["expected_present"] += pk_expected_present(sent, keys, delete_every)
        # ev is the last version sent for the key, which is a delete when the key must not exist.
        is_delete = ("(ev + id - {base}) % {d} = {d} - 1" if delete_every else "FALSE").format(base=base, d=delete_every)
        for table in tables:
            query = ("SELECT COUNT(*), SUM(IF({deleted}, 1, 0)), SUM(IF(NOT ({deleted}) AND ver = ev, 1, 0)), "
                     "SUM(IF(NOT ({deleted}) AND ver < ev, 1, 0)), SUM(IF(ver > ev, 1, 0)), "
                     "SUM(IF(name != CONCAT('n', id, '-', ver) OR score != ver % 100, 1, 0)) "
                     "FROM (SELECT id, ver, name, score, ({last} - (id - {base})) DIV {keys} AS ev FROM {db}.{table} "
                     "WHERE id BETWEEN {base} AND {top}) t").format(
                deleted=is_delete, last=sent - 1, base=base, keys=keys, db=SR_DB, table=table, top=base + keys - 1)
            row = sql(query, timeout=600)[1][0]
            for name, value in zip(("present", "resurrected", "correct", "stale", "ahead", "torn"), row):
                totals[name] += int(value or 0)
    # A key that must exist but has no row was deleted by a delete that came too late.
    totals["missing"] = totals["expected_present"] - totals["correct"] - totals["stale"]
    totals["consistent"] = all(totals[k] == 0 for k in ("stale", "resurrected", "ahead", "torn", "missing"))
    return totals


def log_offsets():
    offsets = {}
    for n in (1, 2, 3):
        path = os.path.join(WORK, "logs", "connect-%d" % n, "connectDistributed.out")
        offsets[path] = os.path.getsize(path) if os.path.exists(path) else 0
    return offsets


def connector_errors(name, offsets):
    """The load failures the tasks of the connector logged since the offsets, grouped by message."""
    errors = {}
    pattern = re.compile(r"preCommit flush fail err (.*?) retry times")
    for path, offset in offsets.items():
        if not os.path.exists(path):
            continue
        with open(path, errors="replace") as f:
            f.seek(offset)
            for line in f:
                if "[%s|" % name not in line:
                    continue
                if "preCommit flush fail" in line:
                    # The SDK exceptions span several lines, so keep the first line from the exception class on.
                    match = pattern.search(line)
                    message = match.group(1) if match else line.split("preCommit flush fail err ", 1)[-1]
                    message = re.sub(r"label: \S+|\d{4,}", "N", message.strip())[:160]
                elif "sink task will be stopped" in line:
                    message = "put failed after sink.maxretries, task stopped"
                else:
                    continue
                errors[message] = errors.get(message, 0) + 1
    return dict(sorted(errors.items(), key=lambda kv: -kv[1])[:8])


def summarize(run, faults, produce_end, drained, drain_s, sent, failures, count, distinct, errors, pk=None):
    first = faults[0][0] if faults else None
    last = faults[-1][0] if faults else None
    settle = run.args.settle_seconds
    samples = [(t, lag) for t, lag, _ in run.freshness if settle <= t <= produce_end]
    base = [lag for t, lag in samples if lag is not None and t < (first if first is not None else produce_end)]
    after = [(t, lag) for t, lag in samples if first is not None and t >= first]
    base_p99 = percentile(base, 99)
    summary = {
        "name": run.name, "mode": run.args.mode, "tasks": run.args.tasks, "tables": len(run.tables),
        "schedule": run.args.schedule, "faults": ["%s:%s" % f for f in faults], "extra": json.loads(run.args.extra),
        "baseline_lag_ms": {"p50": percentile(base, 50), "p99": base_p99},
        "lag_after_fault_ms": None, "impact_s": None, "recovered_after_last_fault_s": None,
    }
    if after:
        lags = [lag for _, lag in after if lag is not None]
        summary["lag_after_fault_ms"] = {"p50": percentile(lags, 50), "p99": percentile(lags, 99),
                                         "max": max(lags) if lags else None,
                                         "unknown_samples": sum(1 for _, lag in after if lag is None)}
        # Recovered: the freshness is back near the baseline and stays there for 30 s.
        threshold = max(2 * (base_p99 or 0), (base_p99 or 0) + 2000)
        ok_since = None
        for t, lag in after:
            if t < last:
                continue
            if lag is not None and lag <= threshold:
                ok_since = t if ok_since is None else ok_since
                if t - ok_since >= 30:
                    summary["impact_s"] = round(ok_since - first, 1)
                    summary["recovered_after_last_fault_s"] = round(ok_since - last, 1)
                    break
            else:
                ok_since = None
    rates = []
    for a, b in zip(run.series, run.series[1:]):
        if a.get("loaded") is not None and b.get("loaded") is not None and b["t"] <= produce_end:
            rates.append((b["t"], (b["loaded"] - a["loaded"]) / max(0.1, b["t"] - a["t"])))
    base_rates = [r for t, r in rates if settle <= t < (first if first is not None else produce_end)]
    after_rates = [r for t, r in rates if first is not None and t >= first]
    summary["loaded_rows_s"] = {
        "baseline": round(sum(base_rates) / len(base_rates)) if base_rates else None,
        "min_5s_after_fault": round(min(after_rates)) if after_rates else None,
        "max_5s_after_fault": round(max(after_rates)) if after_rates else None,
    }
    cs = [p["cs"] for p in run.series if p.get("cs") is not None]
    summary["max_compaction_score"] = max(cs) if cs else None
    end_base = first if first is not None else produce_end
    summary["sr_cpu_cores"] = {"baseline": cpu_cores(run.slow, settle, end_base),
                               "after_fault": cpu_cores(run.slow, first, produce_end) if first is not None else None}
    heaps = [mb for p in run.slow for mb in p["heap_mb"].values()]
    summary["max_worker_heap_mb"] = max(heaps) if heaps else None
    summary["tasks_failed"] = len(run.failed_tasks)
    summary["task_restarts"] = run.restarts
    if pk:
        summary["data"] = dict(pk, sent=sent, producer_failures=failures, drained=drained, drain_s=drain_s)
    else:
        summary["data"] = {"sent": sent, "producer_failures": failures, "loaded_rows": count,
                           "distinct_ids": distinct, "missing": sent - failures - distinct,
                           "duplicates": count - distinct, "drained": drained, "drain_s": drain_s}
    summary["keys"], summary["delete_every"] = run.args.keys, run.args.delete_every
    summary["connector_errors"] = errors
    summary["events"] = run.events
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--name", required=True)
    parser.add_argument("--mode", choices=["merge", "default"], required=True)
    parser.add_argument("--tasks", type=int, required=True)
    parser.add_argument("--schedule", required=True, help="rows/s:seconds,... of all producers together")
    parser.add_argument("--tables", type=int, default=1, help="tables, each with its own topic")
    parser.add_argument("--partitions", type=int, default=96, help="partitions of each topic")
    parser.add_argument("--buckets", type=int, default=16)
    parser.add_argument("--table-type", choices=["dup", "pk"], default="dup")
    parser.add_argument("--producers", type=int, default=8)
    parser.add_argument("--pad", type=int, default=150)
    parser.add_argument("--flush", type=int)
    parser.add_argument("--window", type=int, default=1000)
    parser.add_argument("--extra", default="{}", help="extra connector config as json")
    parser.add_argument("--fault", action="append", default=[], help="seconds:action[:argument]")
    parser.add_argument("--restart-failed-after", type=float, default=60,
                        help="restart tasks that stay FAILED this long, negative to never restart")
    parser.add_argument("--settle-seconds", type=int, default=60, help="ignored at the start of the run")
    parser.add_argument("--drain-timeout", type=int, default=600)
    parser.add_argument("--retention-minutes", type=int, default=30, help="how long Kafka keeps the rows")
    parser.add_argument("--keys", type=int, default=0,
                        help="update this many keys of each producer in turn with a growing version, and check "
                             "at the end that a primary key table has the last version of every key")
    parser.add_argument("--delete-every", type=int, default=0,
                        help="with --keys, every this many versions of a key delete it")
    parser.add_argument("--merge-condition", action="store_true",
                        help="load with merge_condition on the version, so an older update cannot overwrite a newer")
    parser.add_argument("--keep", action="store_true", help="keep the tables and topics")
    args = parser.parse_args()
    if args.restart_failed_after is not None and args.restart_failed_after < 0:
        args.restart_failed_after = None

    name = "st_" + args.name.replace("-", "_")
    tables = [name] if args.tables == 1 else ["%s_%d" % (name, i) for i in range(args.tables)]
    topics = tables
    schedule = [(int(r), int(s)) for r, s in (step.split(":") for step in args.schedule.split(","))]
    faults = sorted((float(f.split(":", 1)[0]), f.split(":", 1)[1]) for f in args.fault)
    if not ensure_cluster_healthy():
        raise SystemExit("StarRocks is not healthy before the run")

    compile_producer()
    sql("CREATE DATABASE IF NOT EXISTS %s" % SR_DB)
    delete(name)
    for table in tables:
        sql("DROP TABLE IF EXISTS %s.%s FORCE" % (SR_DB, table))
        if args.table_type == "pk":
            ddl = ("CREATE TABLE %s.%s (id BIGINT NOT NULL, ver BIGINT, kafka_ts BIGINT, name VARCHAR(64), "
                   "score INT NOT NULL, pad VARCHAR(512)) PRIMARY KEY(id)")
        else:
            ddl = ("CREATE TABLE %s.%s (kafka_ts BIGINT, id BIGINT NOT NULL, ver BIGINT, name VARCHAR(64), "
                   "score INT NOT NULL, pad VARCHAR(512)) DUPLICATE KEY(kafka_ts, id)")
        sql((ddl + " DISTRIBUTED BY HASH(id) BUCKETS %d") % (SR_DB, table, args.buckets))
        delete_topic(table)
    time.sleep(3)
    for topic in topics:
        create_topic(topic, args.partitions, {"retention.ms": args.retention_minutes * 60000,
                                              "segment.bytes": 268435456})

    run = Run(args, name, tables, topics)
    offsets = log_offsets()
    extra = json.loads(args.extra)
    if args.merge_condition:
        extra["sink.properties.merge_condition"] = "ver"
    config = connector_config(topics, tables, args.tasks, args.mode, args.flush, args.window, extra)
    deploy(name, config)
    run.producers = start_producers(",".join(topics), schedule, args.producers, args.pad, name, args.keys,
                                    args.delete_every)
    run.start = time.time()
    threads = [threading.Thread(target=run.sample_freshness, daemon=True),
               threading.Thread(target=run.sample_series, daemon=True),
               threading.Thread(target=run.sample_slow, daemon=True),
               threading.Thread(target=run.inject, args=(faults,), daemon=True)]
    for thread in threads:
        thread.start()
    run.event("started %s, %d tasks, %d tables, schedule %s" % (args.mode, args.tasks, len(tables), args.schedule))

    deadline = time.time() + sum(s for _, s in schedule) + 300
    for _, process in run.producers:
        try:
            process.wait(timeout=max(1.0, deadline - time.time()))
        except subprocess.TimeoutExpired:
            process.kill()
    produce_end = run.now()
    run.event("producers done")

    # Wait until the connector has committed everything it consumed and loaded nothing new for 15 s.
    drained, drain_start, stable_since, last_loaded = False, time.time(), None, None
    while time.time() - drain_start < args.drain_timeout:
        try:
            lag = consumer_lag("connect-" + name)
            loaded = sum(sql("SELECT COUNT(*) FROM %s.%s" % (SR_DB, t), timeout=60)[1][0][0] for t in tables)
        except Exception:  # noqa: BLE001
            lag = loaded = None
        if lag == 0 and loaded is not None and loaded == last_loaded:
            stable_since = stable_since or time.time()
            if time.time() - stable_since >= 15:
                drained = True
                break
        else:
            stable_since = None
        last_loaded = loaded
        time.sleep(5)
    drain_s = round(time.time() - drain_start, 1)
    run.event("drained" if drained else "not drained after %d s" % args.drain_timeout)
    run.stop.set()
    for thread in threads:
        thread.join(timeout=60)

    failures = 0
    for k in range(args.producers):
        try:
            text = open(os.path.join(WORK, "logs", "producer-%s-%d.log" % (name, k))).read()
            failures += int(re.search(r"failures (\d+)", text).group(1))
        except (OSError, AttributeError):
            pass
    count = distinct = 0
    pk = None
    if args.keys:
        # Check the last version of every key now, and again later in case a write that was in flight lands late.
        sent_by_producer = produced_by_each(run.producers)
        pk = pk_check(tables, sent_by_producer, args.keys, args.delete_every)
        run.event("primary key check: %s" % json.dumps(pk))
        time.sleep(60)
        pk["after_60s"] = pk_check(tables, sent_by_producer, args.keys, args.delete_every)
        run.event("primary key check after 60 s: %s" % json.dumps(pk["after_60s"]))
    else:
        # The ids of each producer are dense, so a bitmap counts the distinct ids of billions of rows cheaply.
        for table in tables:
            _, rows = sql("SELECT COUNT(*), BITMAP_UNION_COUNT(TO_BITMAP(id)) FROM %s.%s WHERE id < %d"
                          % (SR_DB, table, BAD_ID_BASE), timeout=900)
            count += rows[0][0]
            distinct += rows[0][1]
    summary = summarize(run, faults, produce_end, drained, drain_s, produced(run.producers), failures, count,
                        distinct, connector_errors(name, offsets), pk)
    print(json.dumps({k: v for k, v in summary.items() if k != "events"}), flush=True)
    with open(os.path.join(WORK, "stability-results.jsonl"), "a") as f:
        f.write(json.dumps(summary) + "\n")
    with open(os.path.join(WORK, "stability-%s.json" % name), "w") as f:
        json.dump({"freshness": run.freshness, "series": run.series, "slow": run.slow, "events": run.events}, f)

    for _, process in run.producers:
        process.kill()
    delete(name)
    if "net_delay" in run.used or "net_loss" in run.used:
        for host in CN_HOSTS:
            ssh(host, "sudo -n /usr/sbin/tc qdisc del dev $(/usr/sbin/ip route show default | awk '{print $5; exit}') root")
    if "compaction_off" in run.used:
        set_fe_config(compaction_disable_key(), "")
    if not ensure_cluster_healthy():
        print("WARNING: StarRocks is not healthy after the run", flush=True)
    if not args.keep:
        for table in tables:
            delete_topic(table)
            sql("DROP TABLE IF EXISTS %s.%s FORCE" % (SR_DB, table))


if __name__ == "__main__":
    main()
