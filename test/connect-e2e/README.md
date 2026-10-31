# Kafka Connect end-to-end and performance tests

These scripts run the connector in a real Kafka Connect cluster against a StarRocks cluster: a single-node
KRaft broker and one or more distributed Connect workers on the local host, without Docker. Merge commit
needs StarRocks 3.4 or later.

## Setup

- A Kafka 3.x distribution, e.g. `kafka_2.13-3.7.2`, and JDK 11 or 17 to run it.
- `pip install pymysql`.
- The connector built with `mvn package`, which produces `target/*-with-dependencies.jar`.

```bash
export KAFKA_HOME=/path/to/kafka_2.13-3.7.2
export SR_FE=<fe host>        # SR_HTTP_PORT, SR_QUERY_PORT, SR_USER, SR_PASSWORD, SR_DB are optional
export WORK=/tmp/starrocks-kafka-connect-e2e   # broker data, logs and results
./kafka_up.sh 1                # broker and worker 1, or ./kafka_up.sh 1 2 3 for three workers
./kafka_down.sh                # stops the broker and the workers of $WORK
```

The broker listens on 39092 and worker n on 38083 + n - 1. `BROKER_PORT`, `CONTROLLER_PORT` and
`CONNECT_PORT_1` change them, and `OFFSET_FLUSH_INTERVAL_MS` (10 s) how often the workers commit offsets.

## Functional tests

```bash
python3 connect_e2e.py         # or a subset: load_dup load_pk restart_rebalance
```

For both the default load path and merge commit:

- `load_dup`, `load_pk`: load 1,000,000 rows with unique ids into a duplicate key and a primary key table, and
  check the count, the distinct ids and a checksum.
- `restart_rebalance`: load 2,000,000 rows at 50,000 rows/s into a primary key table while restarting all
  tasks, adding a second worker and killing it with SIGKILL. Every row must end up exactly once and all tasks
  must be running.

## Performance comparison

```bash
./perf_matrix.sh               # RATE (100000 rows/s), DURATION (180 s) and BACKLOG_ROWS change the load
python3 perf_report.py         # markdown tables of $WORK/perf-results.jsonl
```

Each run creates a table and a 32-partition topic, produces rows of about 250 bytes at a fixed rate, and
drops both at the end. The connector adds the Kafka timestamp of each record with `InsertField`, and
`perf_run.py` measures:

- freshness: every 0.5 s, now minus the newest Kafka timestamp visible in the table, after a 30 s warm-up;
- transactions: the growth of the partition's visible version;
- the maximum compaction score of the table, from `information_schema.partitions_meta`;
- the CPU of the FE and BE/CN processes, when `SR_SSH_USER`, `SR_CN` (comma-separated hosts) and optionally
  `SR_SSH_PASSWORD` are set;
- for the backlog runs, how fast the connector loads a topic that is already full.

## Maximum throughput

```bash
./kafka_up.sh 1 2 3
python3 ramp_run.py --name mc-1s --mode merge --tasks 96 --partitions 96 --producers 8 \
    --rates 1600000,2000000,2400000,2800000,3200000,3600000 --flush 200 --window 1000
```

`ramp_run.py` steps up the rate of one connector to find the highest rate it sustains. Several
`StepProducer` processes follow the same schedule of rates without a gap between the steps (`--step-seconds`,
90 s). For each step it prints and appends to `$WORK/ramp-results.jsonl` the produced and loaded rows/s, the
backlog at the end of the step, the freshness percentiles after the first 30 s of the step, the transactions,
the maximum compaction score and the CPU of the FE, the BE/CN and the local Connect workers, broker and
producers. It stops once the backlog exceeds 60 s of the current rate (`--max-backlog-seconds`). `--mode
default` uses the default load path with `--flush` (1000 ms) as the flush interval, `--extra` adds connector
settings as json.

The producers, the broker and the workers share the CPU of the local host, so watch the client CPU of the
steps: when the host runs out of CPU, the rate a step sustains measures the host, not StarRocks.

## Stability under faults

```bash
export SR_FE=<fe1>,<fe2>,<fe3> SR_CN=<cn1>,<cn2>,<cn3> SR_SSH_USER=<user> SR_SSH_PASSWORD=<password>
python3 stability_run.py --name cn-kill --mode merge --tasks 48 --flush 200 --window 1000 \
    --schedule 800000:600 --fault 180:cn_kill:<cn1> --fault 240:cn_start:<cn1>
python3 stability_report.py    # markdown table of the runs in $WORK/stability-results.jsonl
```

`stability_run.py` runs one connector at a steady load and injects faults at given seconds: kill, stop or start a
CN or an FE (`leader` and `follower` pick one), kill or start a Connect worker, add network delay or packet loss on
a host with `tc netem` (needs passwordless sudo), stop and resume compaction of the tables, set an FE config, send
rows that violate NOT NULL, and pause or resume the connector. `--tables` spreads the load over several tables,
each with its own topic. The faults and the options are listed in `stability_run.py --help`.

During the run it samples the freshness every second (the worst table), and every 5 s the loaded rows, the task
states, the compaction score and the alive nodes, and every 30 s the CPU of the FEs and CNs and the heap of the
workers. A task that stays FAILED for `--restart-failed-after` seconds (60) is restarted, as an operator would do.
After the producers finish, the run waits until the connector has committed everything, and counts with a bitmap
whether every produced id is in the tables and how many rows are duplicates. It appends a summary to
`$WORK/stability-results.jsonl` and saves the time series to `$WORK/stability-<name>.json`, from which
`stability_report.py` computes the longest disruption, the time nothing was loaded, the failed tasks, the missing
rows and the duplicates of each run.

`--table-type pk --keys N` checks the final consistency of a primary key table instead: each producer updates N
keys in turn with a growing version (`ver`), so the last version of every key follows from the rows sent. With
`--delete-every D`, every D-th version of a key deletes it, staggered so that about one key in D ends deleted.
`--merge-condition` loads with `merge_condition` on the version. After the run, and again 60 s later, the script
counts the keys that hold an older version (stale), that exist although their last version deletes them
(resurrected), and that are gone although their last version is an upsert (missing). Few keys, such as 10,000 per
producer at 50,000 rows/s, put several versions of a key in flight at once, which is where an order can break.

```bash
python3 stability_run.py --name pk-hot --mode merge --tasks 48 --flush 200 --table-type pk --keys 10000 \
    --delete-every 5 --schedule 400000:480
```

The StarRocks hosts are reached over SSH, with StarRocks under `SR_HOME` (`/home/disk1/sr`). The start scripts run
in a login shell, which has the `JAVA_HOME` they need. Before and after every run, the script starts any FE or CN
that is down and waits until all of them are alive.
