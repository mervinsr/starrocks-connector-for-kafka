#!/usr/bin/env bash
# Runs the performance comparison one run at a time, with a quiet minute between runs.
# Results are appended to $WORK/perf-results.jsonl; print them with: python3 perf_report.py
source "$(dirname "$0")/env.sh"
cd "$(dirname "$0")"
mkdir -p "$WORK/logs"
RATE=${RATE:-100000}
DURATION=${DURATION:-180}
BACKLOG_ROWS=${BACKLOG_ROWS:-30000000}
run() {
  echo "===== $(date +%T) run $*" | tee -a "$WORK/logs/perf_matrix.log"
  python3 perf_run.py "$@" 2>&1 | grep -v "^SLF4J" >> "$WORK/logs/perf_matrix.log"
  sleep 60
}
run --name dflt-t8     --mode default --tasks 8  --rate "$RATE" --duration "$DURATION"
run --name mc-t8       --mode merge   --tasks 8  --rate "$RATE" --duration "$DURATION" --flush 200 --window 1000
run --name dflt-t32    --mode default --tasks 32 --rate "$RATE" --duration "$DURATION"
run --name mc-t32      --mode merge   --tasks 32 --rate "$RATE" --duration "$DURATION" --flush 200 --window 1000
run --name mcfast-t32  --mode merge   --tasks 32 --rate "$RATE" --duration "$DURATION" --flush 100 --window 500
run --name dflt5s-t32  --mode default --tasks 32 --rate "$RATE" --duration "$DURATION" --flush 5000
run --name pkdflt-t32  --mode default --tasks 32 --rate "$RATE" --duration "$DURATION" --table-type pk
run --name pkmc-t32    --mode merge   --tasks 32 --rate "$RATE" --duration "$DURATION" --flush 200 --window 1000 --table-type pk
run --name maxdflt-t32 --mode default --tasks 32 --rate 0 --rows "$BACKLOG_ROWS" --backlog
run --name maxmc-t32   --mode merge   --tasks 32 --rate 0 --rows "$BACKLOG_ROWS" --backlog --flush 200 --window 1000
echo "===== $(date +%T) matrix done" | tee -a "$WORK/logs/perf_matrix.log"
