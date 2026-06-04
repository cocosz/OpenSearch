#!/bin/bash
# Deploy current branch to EC2 instance with ClickBench data and benchmark Liquid Cache.
#
# Prerequisites:
#   - AWS SSM access to the instance
#   - Instance has Rust toolchain, ClickBench data ingested
#   - Run from the OpenSearch repo root
#
# Usage:
#   ./scripts/bench-liquid-cache.sh

set -euo pipefail

INSTANCE_ID="i-0f5edeb6f19c263b0"
REGION="us-east-1"
NLB="opense-clust-fa6urNP6f3nS-1fba160beba9db5f.elb.us-east-1.amazonaws.com"
OPENSEARCH_HOME="/home/ec2-user/opensearch"
BRANCH="pr-21766"
REPO_URL="https://github.com/Bukhtawar/OpenSearch.git"

ssm() {
    local cmd="$1"
    local timeout="${2:-120}"
    local cmd_id
    cmd_id=$(aws ssm send-command \
        --region "$REGION" \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters "{\"commands\":[\"$cmd\"],\"executionTimeout\":[\"$timeout\"]}" \
        --query "Command.CommandId" --output text)
    echo "  [SSM] cmd=$cmd_id"

    local deadline=$((SECONDS + ${timeout} + 30))
    while [ $SECONDS -lt $deadline ]; do
        sleep 5
        local status
        status=$(aws ssm get-command-invocation \
            --region "$REGION" \
            --command-id "$cmd_id" \
            --instance-id "$INSTANCE_ID" \
            --query "Status" --output text 2>/dev/null || echo "Pending")
        if [[ "$status" == "Success" ]]; then
            aws ssm get-command-invocation \
                --region "$REGION" \
                --command-id "$cmd_id" \
                --instance-id "$INSTANCE_ID" \
                --query "StandardOutputContent" --output text
            return 0
        elif [[ "$status" == "Failed" || "$status" == "TimedOut" ]]; then
            echo "  [SSM] FAILED ($status)"
            aws ssm get-command-invocation \
                --region "$REGION" \
                --command-id "$cmd_id" \
                --instance-id "$INSTANCE_ID" \
                --query "StandardErrorContent" --output text
            return 1
        fi
    done
    echo "  [SSM] TIMEOUT"
    return 1
}

echo "=== Liquid Cache Benchmark Deploy ==="
echo "  Instance: $INSTANCE_ID"
echo "  Branch:   $BRANCH"
echo ""

# Step 1: Clone/update repo on instance
echo "1. Pulling latest code on instance..."
ssm "cd /home/ec2-user && \
    if [ -d OpenSearch-bench ]; then \
        cd OpenSearch-bench && git fetch origin && git checkout $BRANCH && git pull origin $BRANCH; \
    else \
        git clone --depth 1 -b $BRANCH $REPO_URL OpenSearch-bench; \
    fi && echo DONE" 300

# Step 2: Build native library on instance (Linux arm64)
echo ""
echo "2. Building native library (this takes ~5-10 min)..."
ssm "cd /home/ec2-user/OpenSearch-bench/sandbox/libs/dataformat-native/rust && \
    cargo build --release -p opensearch-native-lib 2>&1 | tail -3 && echo BUILD_DONE" 900

# Step 3: Build OpenSearch distribution
echo ""
echo "3. Building OpenSearch distribution..."
ssm "cd /home/ec2-user/OpenSearch-bench && \
    ./gradlew :distribution:archives:linux-arm64-tar:assemble -Dsandbox.enabled=true -x javadoc -x test 2>&1 | tail -5 && echo DIST_DONE" 900

# Step 4: Stop OpenSearch, replace native lib, restart
echo ""
echo "4. Deploying to running cluster..."
ssm "sudo pkill -9 -f opensearch || true; sleep 2; \
    cp /home/ec2-user/OpenSearch-bench/sandbox/libs/dataformat-native/rust/target/release/libopensearch_native.so \
       $OPENSEARCH_HOME/lib/libopensearch_native.so && \
    echo LIB_REPLACED" 60

echo "   Restarting OpenSearch..."
ssm "setsid sudo -u ec2-user $OPENSEARCH_HOME/opensearch-tar-install-datafusion.sh >> /home/ec2-user/install.log 2>&1 & echo STARTED" 30

echo "   Waiting for cluster..."
for i in $(seq 1 18); do
    sleep 5
    if curl -s "http://$NLB:9200/_cluster/health" | grep -q '"status":"green"'; then
        echo "   Cluster is GREEN"
        break
    fi
    echo "   ... waiting ($i)"
done

# Step 5: Run benchmark queries
echo ""
echo "=== Running Benchmark ==="
echo ""

QUERIES=(
    "source=clickbench | where URLDomain = 'google.com' | stats count() as cnt"
    "source=clickbench | where UserID > 0 | stats avg(ResolutionWidth) as avg_w"
    "source=clickbench | stats count() as cnt by RegionID"
    "source=clickbench | where EventTime > 1700000000 | stats count() as cnt by URLDomain"
    "source=clickbench | where IsRobot = 1 | stats count() as cnt"
)

run_queries() {
    local label="$1"
    echo "--- $label ---"
    for i in "${!QUERIES[@]}"; do
        local q="${QUERIES[$i]}"
        local total=0
        for iter in $(seq 1 5); do
            local start=$(date +%s%N)
            curl -s -X POST "http://$NLB:9200/_plugins/_ppl" \
                -H "Content-Type: application/json" \
                -d "{\"query\":\"$q\"}" > /dev/null
            local end=$(date +%s%N)
            local elapsed=$(( (end - start) / 1000000 ))
            total=$((total + elapsed))
        done
        local avg=$((total / 5))
        echo "  Q$((i+1)): ${avg}ms  ($q)"
    done
    echo ""
}

# Warmup
echo "Warming up..."
for q in "${QUERIES[@]}"; do
    curl -s -X POST "http://$NLB:9200/_plugins/_ppl" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"$q\"}" > /dev/null
done

# LC OFF
echo ""
curl -s -X PUT "http://$NLB:9200/_cluster/settings" \
    -H "Content-Type: application/json" \
    -d '{"transient":{"datafusion.liquid_cache.enabled":"false"}}' > /dev/null
sleep 2
run_queries "LC OFF (baseline)"

# LC ON - cold
curl -s -X PUT "http://$NLB:9200/_cluster/settings" \
    -H "Content-Type: application/json" \
    -d '{"transient":{"datafusion.liquid_cache.enabled":"true"}}' > /dev/null
sleep 2
run_queries "LC ON (cold cache)"

# LC ON - warm
run_queries "LC ON (warm cache)"

echo "=== Done ==="
