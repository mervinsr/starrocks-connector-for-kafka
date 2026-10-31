"""Helpers of the Kafka Connect test harness: StarRocks SQL, Connect REST, topics and the producer."""
import json
import os
import subprocess
import time
import urllib.request

import pymysql

HARNESS = os.path.dirname(os.path.abspath(__file__))
KAFKA_HOME = os.environ["KAFKA_HOME"]
BROKER = os.environ["BROKER"]
# SR_FE may list several FEs, comma-separated. Queries go to the first one that answers.
SR_FES = [h for h in os.environ["SR_FE"].split(",") if h]
SR_FE = SR_FES[0]
SR_HTTP_PORT = int(os.environ["SR_HTTP_PORT"])
SR_QUERY_PORT = int(os.environ["SR_QUERY_PORT"])
SR_DB = os.environ["SR_DB"]
SR_USER = os.environ.get("SR_USER", "root")
SR_PASSWORD = os.environ.get("SR_PASSWORD", "")
CONNECT_PORT = int(os.environ.get("CONNECT_PORT_1", "38083"))


_live_fe = [0]


def sql(query, db=None, host=None, timeout=120):
    """Runs a query on the given FE, or on the first FE that accepts a connection."""
    if host:
        hosts = [host]
    else:
        hosts = SR_FES[_live_fe[0]:] + SR_FES[:_live_fe[0]]
    error = None
    for h in hosts:
        try:
            conn = pymysql.connect(host=h, port=SR_QUERY_PORT, user=SR_USER, password=SR_PASSWORD, database=db,
                                   autocommit=True, connect_timeout=5, read_timeout=timeout)
        except pymysql.err.OperationalError as e:
            error = e
            continue
        if not host:
            _live_fe[0] = SR_FES.index(h)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [d[0] for d in cur.description] if cur.description else []
                return columns, cur.fetchall()
        finally:
            conn.close()
    raise error


def scalar(query):
    return sql(query)[1][0][0]


def visible_version(table):
    columns, rows = sql("SHOW PARTITIONS FROM %s.%s" % (SR_DB, table))
    index = columns.index("VisibleVersion")
    return sum(int(row[index]) for row in rows)


def compaction_score(table):
    _, rows = sql("SELECT MAX(MAX_CS), MAX(AVG_CS) FROM information_schema.partitions_meta "
                  "WHERE DB_NAME = '%s' AND TABLE_NAME = '%s'" % (SR_DB, table))
    return float(rows[0][0] or 0), float(rows[0][1] or 0)


def rest(method, path, body=None, port=None):
    """Calls the Connect REST API of the given worker, or of the first of three workers that answers."""
    data = json.dumps(body).encode() if body is not None else None
    error = None
    for p in [port] if port else [CONNECT_PORT + n for n in range(3)]:
        req = urllib.request.Request("http://localhost:%d%s" % (p, path), data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                text = resp.read().decode()
                return json.loads(text) if text else None
        except urllib.error.HTTPError as e:
            raise RuntimeError("%s %s -> %d %s" % (method, path, e.code, e.read().decode()))
        except OSError as e:
            error = e
    raise error


def connector_config(topic, table, tasks, mode, flush_ms=None, window_ms=1000, extra=None):
    """The connector config for a topic and its table, or for lists of topics and tables."""
    topics = topic if isinstance(topic, list) else [topic]
    tables = table if isinstance(table, list) else [table]
    config = {
        "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
        "tasks.max": str(tasks),
        "topics": ",".join(topics),
        "starrocks.http.url": ",".join("%s:%d" % (h, SR_HTTP_PORT) for h in SR_FES),
        "starrocks.database.name": SR_DB,
        "starrocks.username": SR_USER,
        "starrocks.password": SR_PASSWORD,
        "starrocks.topic2table.map": ",".join("%s:%s" % (t, tb) for t, tb in zip(topics, tables)),
        "sink.properties.strip_outer_array": "true",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # The time the producer sent each record, to measure how fresh the loaded data is.
        "transforms": "ts",
        "transforms.ts.type": "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.ts.timestamp.field": "kafka_ts",
    }
    if mode == "merge":
        config["sink.properties.enable_merge_commit"] = "true"
        config["sink.properties.merge_commit_interval_ms"] = str(window_ms)
        config["bufferflush.intervalms"] = str(flush_ms or 200)
    else:
        config["bufferflush.intervalms"] = str(flush_ms or 1000)
    config.update(extra or {})
    return config


def deploy(name, config):
    rest("PUT", "/connectors/%s/config" % name, config)
    for _ in range(120):
        try:
            status = rest("GET", "/connectors/%s/status" % name)
            tasks = status.get("tasks", [])
            if tasks and all(t["state"] == "RUNNING" for t in tasks):
                return status
        except RuntimeError:
            pass
        time.sleep(1)
    raise RuntimeError("connector %s did not start: %s" % (name, rest("GET", "/connectors/%s/status" % name)))


def delete(name):
    try:
        rest("DELETE", "/connectors/%s" % name)
    except RuntimeError:
        pass


def task_states(name):
    status = rest("GET", "/connectors/%s/status" % name)
    return [(t["id"], t["state"], t.get("worker_id")) for t in status.get("tasks", [])]


def create_topic(topic, partitions, configs=None):
    command = [KAFKA_HOME + "/bin/kafka-topics.sh", "--bootstrap-server", BROKER, "--create", "--topic", topic,
               "--partitions", str(partitions), "--replication-factor", "1"]
    for key, value in (configs or {}).items():
        command += ["--config", "%s=%s" % (key, value)]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def delete_topic(topic):
    subprocess.run([KAFKA_HOME + "/bin/kafka-topics.sh", "--bootstrap-server", BROKER, "--delete", "--topic", topic],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start_producer(topic, first, count, rate, pad_bytes, log_file):
    log = open(log_file, "w")
    return subprocess.Popen(["java", "-cp", KAFKA_HOME + "/libs/*", HARNESS + "/RateProducer.java", BROKER, topic,
                             str(first), str(count), str(rate), str(pad_bytes)], stdout=log, stderr=subprocess.STDOUT)


def consumer_lag(group):
    """The records the group has not committed, or None when Kafka does not know the group.

    A partition without a committed offset counts with all its records, so that a group that never committed is
    not taken for one that is done.
    """
    out = subprocess.run([KAFKA_HOME + "/bin/kafka-consumer-groups.sh", "--bootstrap-server", BROKER, "--describe",
                          "--group", group], capture_output=True, text=True).stdout
    lag = partitions = 0
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == group and parts[4].isdigit():
            partitions += 1
            end = int(parts[4])
            lag += end - int(parts[3]) if parts[3].isdigit() else end
    return lag if partitions else None


def wait_until(predicate, timeout_s, interval_s=2.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return predicate()
