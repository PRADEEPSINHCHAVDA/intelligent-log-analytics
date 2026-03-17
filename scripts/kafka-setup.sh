#!/usr/bin/env bash
# =============================================================================
# Kafka Setup Script - Creates topics and verifies the pipeline
# Run this after `docker compose up -d` to initialize Kafka topics
# =============================================================================
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
OPENSEARCH="${OPENSEARCH:-http://localhost:9200}"
PARTITIONS="${PARTITIONS:-6}"
REPLICATION="${REPLICATION:-1}"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║     Log Analytics Platform - Kafka Setup             ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ---------------------------------------------------------------------------
# Wait for Kafka to be ready
# ---------------------------------------------------------------------------
wait_for_kafka() {
    info "Waiting for Kafka at $KAFKA_BOOTSTRAP..."
    local retries=30
    while ! docker exec kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            error "Kafka did not start in time"
        fi
        echo -n "."
        sleep 3
    done
    echo ""
    success "Kafka is ready"
}

# ---------------------------------------------------------------------------
# Wait for OpenSearch
# ---------------------------------------------------------------------------
wait_for_opensearch() {
    info "Waiting for OpenSearch at $OPENSEARCH..."
    local retries=30
    while ! curl -sf "$OPENSEARCH/_cluster/health" >/dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            error "OpenSearch did not start in time"
        fi
        echo -n "."
        sleep 5
    done
    echo ""
    success "OpenSearch is ready"
}

# ---------------------------------------------------------------------------
# Create Kafka topics
# ---------------------------------------------------------------------------
create_topics() {
    info "Creating Kafka topics..."
    local topics=(
        "logs.user-service"
        "logs.payment-service"
        "logs.order-service"
        "logs.notification-service"
        "logs.inventory-service"
        "logs.auth-service"
        "logs.api-gateway-service"
        "logs.reporting-service"
    )

    for topic in "${topics[@]}"; do
        docker exec kafka kafka-topics \
            --create \
            --if-not-exists \
            --bootstrap-server localhost:9092 \
            --replication-factor "$REPLICATION" \
            --partitions "$PARTITIONS" \
            --topic "$topic" 2>&1 | grep -v "already exists" || true
        success "Topic: $topic ($PARTITIONS partitions)"
    done
}

# ---------------------------------------------------------------------------
# Verify topics
# ---------------------------------------------------------------------------
verify_topics() {
    info "Listing all topics:"
    docker exec kafka kafka-topics \
        --list \
        --bootstrap-server localhost:9092 | sort
}

# ---------------------------------------------------------------------------
# Configure OpenSearch index template + ISM policies
# ---------------------------------------------------------------------------
configure_opensearch() {
    info "Configuring OpenSearch..."

    # Index template
    curl -sf -X PUT "$OPENSEARCH/_index_template/logs-template" \
        -H "Content-Type: application/json" \
        -d @"$(dirname "$0")/../scripts/opensearch-index-template.json" \
        >/dev/null && success "Index template created" || warn "Index template may already exist"

    # ISM policy for hot→warm→cold lifecycle
    curl -sf -X PUT "$OPENSEARCH/_plugins/_ism/policies/log-retention" \
        -H "Content-Type: application/json" \
        -d @"$(dirname "$0")/../scripts/opensearch-ism-policy.json" \
        >/dev/null && success "ISM retention policy created" || warn "ISM policy may already exist"
}

# ---------------------------------------------------------------------------
# Verify pipeline end-to-end
# ---------------------------------------------------------------------------
verify_pipeline() {
    info "Sending test log to verify pipeline..."

    # Send a test message via Kafka console producer
    echo '{"timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'","level":"INFO","message":"Pipeline test from kafka-setup.sh","service":"test-service","correlation_id":"test-'$(date +%s)'"}' | \
    docker exec -i kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic logs.user-service 2>/dev/null

    success "Test log sent to Kafka"

    # Wait a moment for consumer to process
    sleep 5

    # Check OpenSearch for the record
    local count
    count=$(curl -sf "$OPENSEARCH/logs-*/_count" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count', 0))" 2>/dev/null || echo "0")

    if [[ "$count" -gt "0" ]]; then
        success "OpenSearch has $count log records indexed"
    else
        warn "No records in OpenSearch yet (consumer may still be starting)"
    fi
}

# ---------------------------------------------------------------------------
# Print access URLs
# ---------------------------------------------------------------------------
print_urls() {
    echo ""
    echo "╔══════════════════════════════════════════════════════╗"
    echo "║              Access Your Platform                    ║"
    echo "╠══════════════════════════════════════════════════════╣"
    echo "║  Kafka UI:        http://localhost:8080              ║"
    echo "║  OpenSearch:      http://localhost:9200              ║"
    echo "║  Dashboards:      http://localhost:5601              ║"
    echo "╠══════════════════════════════════════════════════════╣"
    echo "║  Check logs:      docker compose logs -f             ║"
    echo "║  Stop all:        docker compose down                ║"
    echo "╚══════════════════════════════════════════════════════╝"
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
wait_for_kafka
wait_for_opensearch
create_topics
verify_topics
configure_opensearch
verify_pipeline
print_urls

success "Setup complete! Your log analytics platform is running."
