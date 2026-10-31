#!/usr/bin/env bash
# Starts a single-node KRaft broker and the Connect workers given as arguments (1, 2, ...).
# Worker n listens on CONNECT_PORT_1 + n - 1.
set -euo pipefail
source "$(dirname "$0")/env.sh"
mkdir -p $WORK/plugins/starrocks-kafka-connector $WORK/logs
cp -f $CONNECTOR_JAR $WORK/plugins/starrocks-kafka-connector/
cat > $WORK/server.properties <<CONF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:$CONTROLLER_PORT
listeners=PLAINTEXT://localhost:$BROKER_PORT,CONTROLLER://localhost:$CONTROLLER_PORT
advertised.listeners=PLAINTEXT://$BROKER
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$WORK/kraft-logs
num.partitions=8
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
num.network.threads=8
num.io.threads=16
log.retention.ms=7200000
log.segment.bytes=268435456
CONF
if ! (ss -ltn | grep -q ":$BROKER_PORT "); then
  rm -rf $WORK/kraft-logs
  CID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
  $KAFKA_HOME/bin/kafka-storage.sh format -t $CID -c $WORK/server.properties > $WORK/logs/format.log
  LOG_DIR=$WORK/logs/broker KAFKA_HEAP_OPTS="-Xms4g -Xmx4g" $KAFKA_HOME/bin/kafka-server-start.sh -daemon $WORK/server.properties
  for i in $(seq 1 60); do (ss -ltn | grep -q ":$BROKER_PORT ") && break; sleep 1; done
  echo "broker up"
fi
for n in "$@"; do
  port=$((CONNECT_PORT_1 + n - 1))
  cat > $WORK/connect-$n.properties <<CONF
bootstrap.servers=$BROKER
group.id=sr-connect
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
offset.storage.topic=_connect_offsets
config.storage.topic=_connect_configs
status.storage.topic=_connect_status
offset.storage.replication.factor=1
config.storage.replication.factor=1
status.storage.replication.factor=1
offset.flush.interval.ms=${OFFSET_FLUSH_INTERVAL_MS:-10000}
listeners=http://localhost:$port
rest.advertised.host.name=localhost
rest.advertised.port=$port
plugin.path=$WORK/plugins
connector.client.config.override.policy=All
# Reassign the tasks of a dead worker after 10 s instead of 5 minutes.
scheduled.rebalance.max.delay.ms=10000
CONF
  if ! (ss -ltn | grep -q ":$port "); then
    LOG_DIR=$WORK/logs/connect-$n KAFKA_HEAP_OPTS="-Xms8g -Xmx8g" \
      $KAFKA_HOME/bin/connect-distributed.sh -daemon $WORK/connect-$n.properties
    for i in $(seq 1 90); do curl -sf http://localhost:$port/ >/dev/null 2>&1 && break; sleep 1; done
    echo "connect worker $n up on $port: $(curl -sf http://localhost:$port/connector-plugins | grep -o 'StarRocksSinkConnector' | head -1)"
  fi
done
