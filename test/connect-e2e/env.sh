# Settings of the Kafka Connect end-to-end and performance tests. Override any of them in the environment.
HARNESS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HARNESS/../.." && pwd)"
export HARNESS
: "${KAFKA_HOME:?Set KAFKA_HOME to a Kafka 3.x distribution, e.g. kafka_2.13-3.7.2}"
: "${SR_FE:?Set SR_FE to the StarRocks FE host}"
export KAFKA_HOME SR_FE
export WORK=${WORK:-/tmp/starrocks-kafka-connect-e2e}
export BROKER_PORT=${BROKER_PORT:-39092}
export CONTROLLER_PORT=${CONTROLLER_PORT:-39093}
export CONNECT_PORT_1=${CONNECT_PORT_1:-38083}
export BROKER=localhost:$BROKER_PORT
export CONNECTOR_JAR=${CONNECTOR_JAR:-$(ls "$REPO"/target/starrocks-connector-for-kafka-*-with-dependencies.jar 2>/dev/null | head -1)}
export SR_HTTP_PORT=${SR_HTTP_PORT:-8030}
export SR_QUERY_PORT=${SR_QUERY_PORT:-9030}
export SR_USER=${SR_USER:-root}
export SR_PASSWORD=${SR_PASSWORD:-}
export SR_DB=${SR_DB:-kafka_connect_e2e}
# The Kafka tools start a local JMX agent, which fails when the host runs out of ephemeral ports.
export KAFKA_JMX_OPTS=${KAFKA_JMX_OPTS:-" "}
