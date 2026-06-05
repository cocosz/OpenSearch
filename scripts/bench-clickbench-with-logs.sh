#!/bin/bash
# ClickBench benchmark with per-query log capture.
# Captures LC debug logs (from stderr) per query for analysis.
ITERS=5
QUERY_DIR="/home/ec2-user/OpenSearch-bench/sandbox/plugins/analytics-engine/src/test/resources/clickbench/queries"
OS="http://localhost:9200"
LOG_DIR="/home/ec2-user/bench_logs"
STDERR_LOG="/home/ec2-user/os_all.log"

mkdir -p "$LOG_DIR"

curl -s -X PUT "$OS/_cluster/settings" -H "Content-Type: application/json" \
  -d '{"transient":{"datafusion.liquid_cache.enabled":"true"}}' > /dev/null
sleep 2

echo "=== ClickBench LC ON - ${ITERS} iters, per-query logs ==="
echo "Query | Min(ms) | Times(ms)"
echo "------|---------|----------"

for i in $(seq 1 43); do
  QFILE="$QUERY_DIR/q${i}.ppl"
  if [ ! -f "$QFILE" ]; then continue; fi

  python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    lines = f.readlines()
ppl_lines = []
capture = False
for line in lines:
    s = line.strip()
    if s.startswith('source='):
        capture = True
    if capture and s and not s.startswith('/*') and not s.startswith('*/') and not s.startswith('*'):
        ppl_lines.append(s)
ppl = ' '.join(ppl_lines).replace('source=hits', 'source=clickbench')
json.dump({'query': ppl}, open('/tmp/ppl_query.json', 'w'))
" "$QFILE"

  if [ ! -s /tmp/ppl_query.json ]; then continue; fi

  # Record stderr line count before query
  BEFORE=$(wc -l < "$STDERR_LOG" 2>/dev/null || echo 0)

  TIMES=""
  MIN=999999
  for iter in $(seq 1 $ITERS); do
    START=$(date +%s%N)
    RESP=$(curl -s --max-time 120 -X POST "$OS/_plugins/_ppl" \
      -H "Content-Type: application/json" -d @/tmp/ppl_query.json 2>&1)
    END=$(date +%s%N)
    MS=$(( (END - START) / 1000000 ))
    if echo "$RESP" | grep -q '"status":4\|"status":5'; then MS=-1; fi
    if [ $MS -lt $MIN ] && [ $MS -ge 0 ]; then MIN=$MS; fi
    TIMES="$TIMES $MS"
    sleep 1
  done

  # Record stderr line count after query
  AFTER=$(wc -l < "$STDERR_LOG" 2>/dev/null || echo 0)

  # Extract per-query logs
  if [ "$AFTER" -gt "$BEFORE" ]; then
    sed -n "$((BEFORE+1)),${AFTER}p" "$STDERR_LOG" > "$LOG_DIR/q${i}.log"
  else
    echo "NO LC LOGS" > "$LOG_DIR/q${i}.log"
  fi

  if [ $MIN -eq 999999 ]; then MIN="ERR"; fi
  printf "Q%-2d   | %7s |%s\n" "$i" "$MIN" "$TIMES"
done

echo "=== Done ==="
echo
echo "=== Per-Query LC Summary ==="
for i in $(seq 1 43); do
  if [ -f "$LOG_DIR/q${i}.log" ]; then
    OPT=$(grep -c "LC-Optimizer" "$LOG_DIR/q${i}.log" 2>/dev/null)
    OPEN=$(grep -c "LC-Opener" "$LOG_DIR/q${i}.log" 2>/dev/null)
    HITS=$(grep "LC-Reader" "$LOG_DIR/q${i}.log" 2>/dev/null | grep -c "hits=[1-9]")
    MISSES=$(grep "LC-Reader" "$LOG_DIR/q${i}.log" 2>/dev/null | grep -c "misses=[1-9]")
    FILTER=$(grep -c "row_filter" "$LOG_DIR/q${i}.log" 2>/dev/null)
    if [ "$OPT" -gt 0 ] || [ "$OPEN" -gt 0 ] || [ "$HITS" -gt 0 ] || [ "$MISSES" -gt 0 ]; then
      printf "Q%-2d: optimizer=%s, opener=%s, hits=%s, misses=%s, filter=%s\n" \
        "$i" "$OPT" "$OPEN" "$HITS" "$MISSES" "$FILTER"
    else
      printf "Q%-2d: NO LC (skipped)\n" "$i"
    fi
  fi
done
