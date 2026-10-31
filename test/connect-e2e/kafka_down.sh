#!/usr/bin/env bash
# Stops the Connect workers given as arguments, or the workers and the broker started by kafka_up.sh.
# Only processes started with this $WORK are stopped, so other Kafka processes on the host are left alone.
source "$(dirname "$0")/env.sh"
if [ $# -gt 0 ]; then
  for n in "$@"; do pkill -f "$WORK/connect-$n.properties" || true; done
  exit 0
fi
pkill -f "$WORK/connect-" || true
sleep 3
pkill -f "$WORK/server.properties" || true
for i in $(seq 1 30); do pgrep -f "$WORK/server.properties" >/dev/null || break; sleep 1; done
