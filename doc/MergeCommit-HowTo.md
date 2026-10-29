# Load Data with Merge Commit

## Background

Without merge commit, every flush of every task is a separate Stream Load transaction. A table therefore gets `tasks × flushes per second` transactions, and each one creates a data version. Lowering `bufferflush.intervalms` for fresher data, or raising `tasks.max` for throughput, multiplies the versions. This drives up compaction, and eventually makes StarRocks throttle the loads.

[Merge commit](https://docs.starrocks.io/docs/loading/StreamLoad/#merge-stream-load-requests), available in StarRocks 3.4 and later, solves this on the server side. StarRocks merges the Stream Load requests that arrive for a table within a time window (`merge_commit_interval_ms`) into one transaction, provided their Stream Load properties are identical. All tasks of a connector send identical properties, so a table gets at most one transaction per window, regardless of the number of tasks and the flush interval.

## Requirements

- StarRocks 3.4 or later.
- The connector built with `starrocks-stream-load-sdk` 1.1-SNAPSHOT. See [How to build](../README.md#how-to-build).

## Quick start

```properties
name=starrocks-sink
connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
topics=orders
tasks.max=8
starrocks.http.url=fe1:8030,fe2:8030
starrocks.database.name=demo
starrocks.username=loader
starrocks.password=******
sink.properties.format=json
sink.properties.enable_merge_commit=true
sink.properties.merge_commit_interval_ms=1000
bufferflush.intervalms=200
```

## Options

### Stream Load properties

| Option | Default | Description |
|---|---|---|
| `sink.properties.enable_merge_commit` | `false` | Load with merge commit. |
| `sink.properties.merge_commit_interval_ms` | (required) | The merge window in milliseconds. A larger window merges more requests into a transaction, at the cost of latency. |
| `sink.properties.merge_commit_parallel` | `3` | The parallelism of the load plan of each merged transaction, capped by the number of BE or CN nodes. |
| `sink.properties.merge_commit_async` | `true` | `true`: StarRocks returns a request once it receives the data. `false`: it returns once the merged transaction is visible. The connector waits until the data is visible before it commits the Kafka offsets in both cases. |

With merge commit and JSON, the connector also sets `strip_outer_array=true` and `ignore_json_size=true` unless you set them. A merge commit request carries its length, so without `ignore_json_size` StarRocks rejects JSON bodies larger than `streaming_load_max_batch_size_mb` (100 MB by default).

### Connector options

| Option | Default | Description |
|---|---|---|
| `bufferflush.intervalms` | `1000` | The flush interval. With merge commit, it can go down to `100`; without merge commit, the minimum stays `1000`. Keep it at most `merge_commit_interval_ms`, so that every task sends data in every window. |
| `sink.merge-commit.chunk.size` | 20971520, or 524288000 when requests are sent one at a time | The maximum bytes of a table sent in one request. |
| `sink.merge-commit.max-concurrent-requests` | `2147483647` | The maximum number of inflight requests of a table in a task. `0` or `1` sends one request at a time, which keeps the order of the records. A negative value means no limit. Only no limit is accepted for now, see [Ordering for primary key tables](#ordering-for-primary-key-tables). |
| `sink.merge-commit.backend-direct-connection` | `false` | Send requests to the BE or CN nodes that StarRocks assigns to the table, instead of an FE node, which redirects every request through the leader FE. The nodes must be reachable from Kafka Connect. |
| `sink.merge-commit.http.thread.num` | `3` | Threads that check the state of the merged transactions. |
| `sink.merge-commit.http.max-connections` | `20` | Maximum HTTP connections to a StarRocks node. |
| `sink.merge-commit.http.total-max-connections` | `1000` | Maximum HTTP connections to StarRocks. |
| `sink.merge-commit.http.idle-connection-timeout-ms` | `60000` | How long an idle HTTP connection is kept. |
| `sink.merge-commit.node-meta.update-interval-ms` | `5000` | How often to refresh the nodes assigned to a table, with `backend-direct-connection`. |
| `sink.merge-commit.check-label-state.init-delay-ms` | `500` | How long after the merge window closes to first check a merged transaction. |
| `sink.merge-commit.check-label-state.interval-ms` | `500` | How often to check a merged transaction until it is visible. |
| `sink.merge-commit.check-label-state.timeout-ms` | the Stream Load `timeout` | How long to wait for a merged transaction to become visible. |
| `sink.publish-timeout.ms` | `10000` with merge commit, `-1` otherwise | How long a committed transaction may stay unpublished before the load counts as successful. `-1` uses the StarRocks default. |
| `sink.io.thread-count` | `2` | Threads that send requests to StarRocks in a task. With `merge_commit_async=false`, each request holds a thread for the whole merge window. |

## Semantics

- **At-least-once.** When Kafka Connect commits offsets, the connector flushes and waits until the merged transactions of all data sent so far are visible. If a load fails, the commit fails and Kafka Connect replays the records from the last committed offsets.
- **No exactly-once.** StarRocks generates the labels of merged transactions and ignores the labels of the requests, so replayed records are loaded again. This is the same as without merge commit.
- **A bad row fails the whole window.** If a request fails a merged transaction, for example with a row that does not fit the table while `max_filter_ratio` is 0, every request in that transaction fails, including those of other tasks, and they all replay. `max_filter_ratio` applies to the merged transaction as a whole. If bad rows are expected, set `sink.properties.max_filter_ratio`, and `sink.properties.log_rejected_record_num` to keep the rejected rows.

## Ordering for primary key tables

A task may have several requests of a table in flight. They can land on different nodes in the same transaction, or in different transactions. Updates of the same key may then apply out of order. To keep the order, set `sink.properties.merge_condition` to a version column, so that an older update does not overwrite a newer one.

Sending one request of a table at a time (`sink.merge-commit.max-concurrent-requests=0`) would also keep the order, at the cost of throughput and up to one more window plus publish time of latency. The connector rejects a limit for now: with the SDK at starrocks-connector-for-apache-flink `e0eb030`, a table that reaches its limit silently drops data. The limit will be accepted once the SDK includes [the fix](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/528).

## Latency

The time until a record is visible is roughly:

```
consume + bufferflush.intervalms + up to merge_commit_interval_ms + commit and publish
```

When Kafka Connect commits offsets (every `offset.flush.interval.ms`), the task waits for its inflight loads and does not consume in the meantime.

## Limitations

- All merge commit tasks in a Kafka Connect worker must use the same `starrocks.http.url`, `starrocks.username` and `starrocks.password`. The SDK shares its connections to StarRocks across them and keeps the settings of the first task, so the connector fails a task whose settings differ. Run such connectors in different workers, and restart the worker after you change these settings.
- For the same reason, the HTTP settings `sink.merge-commit.http.*` and `sink.merge-commit.node-meta.update-interval-ms` come from the first merge commit task in a worker.
- Merge commit helps when several tasks or frequent flushes write to a table. A single task with a long flush interval gains nothing and waits up to one more window.
- The SDK logs every request at INFO (`Load request trace`). Set the log level of `com.starrocks.data.load.stream.mergecommit` to `WARN` if that is too verbose.
