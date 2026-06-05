#!/bin/bash
# ClickBench benchmark script with proper JSON handling for multi-line PPL queries.
ITERS=3
QUERY_DIR="/home/ec2-user/OpenSearch-bench/sandbox/plugins/analytics-engine/src/test/resources/clickbench/queries"
OS="http://localhost:9200"

curl -s -X PUT "$OS/_cluster/settings" -H "Content-Type: application/json" \
  -d '{"transient":{"datafusion.liquid_cache.enabled":"true"}}' > /dev/null
sleep 2

echo "=== ClickBench LC ON - ${ITERS} iters ==="
echo "Query | Min(ms) | Times(ms)"
echo "------|---------|----------"

for i in $(seq 1 43); do
  QFILE="$QUERY_DIR/q${i}.ppl"
  if [ ! -f "$QFILE" ]; then continue; fi

  # Use python to safely build JSON body — handles quotes, newlines, special chars
  python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    lines = f.readlines()
ppl_lines = []
capture = False
for line in lines:
    stripped = line.strip()
    if stripped.startswith('source='):
        capture = True
    if capture and stripped and not stripped.startswith('/*') and not stripped.startswith('*/') and not stripped.startswith('*'):
        ppl_lines.append(stripped)
ppl = ' '.join(ppl_lines).replace('source=hits', 'source=clickbench')
json.dump({'query': ppl}, open('/tmp/ppl_query.json', 'w'))
" "$QFILE"

  if [ ! -s /tmp/ppl_query.json ]; then continue; fi

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
  done

  if [ $MIN -eq 999999 ]; then MIN="ERR"; fi
  printf "Q%-2d   | %7s |%s\n" "$i" "$MIN" "$TIMES"
done
echo "=== Done ==="
