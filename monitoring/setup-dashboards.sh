#!/usr/bin/env bash
# =============================================================================
# OpenSearch Dashboards Setup
# Run after docker compose up to create index patterns and visualizations
# =============================================================================
set -euo pipefail

OS="${OPENSEARCH:-http://localhost:9200}"
KIBANA="${KIBANA:-http://localhost:5601}"

GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║  Setting up OpenSearch Dashboards        ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# Wait for Dashboards to be ready
info "Waiting for OpenSearch Dashboards..."
until curl -sf "$KIBANA/api/status" | python3 -c "
import sys, json
d = json.load(sys.stdin)
s = d.get('status', {}).get('overall', {}).get('state', '')
sys.exit(0 if s == 'green' else 1)
" 2>/dev/null; do
    echo -n "."
    sleep 5
done
echo ""
success "OpenSearch Dashboards is ready"

# Create index pattern
info "Creating index pattern: logs-*"
curl -sf -X POST "$KIBANA/api/saved_objects/index-pattern/logs-*" \
    -H "Content-Type: application/json" \
    -H "kbn-xsrf: true" \
    -d '{
      "attributes": {
        "title": "logs-*",
        "timeFieldName": "@timestamp"
      }
    }' >/dev/null && success "Index pattern created" || info "Index pattern may already exist"

# Set default index pattern
info "Setting logs-* as default index pattern..."
curl -sf -X POST "$KIBANA/api/kibana/settings" \
    -H "Content-Type: application/json" \
    -H "kbn-xsrf: true" \
    -d '{"changes": {"defaultIndex": "logs-*"}}' >/dev/null && success "Default index set"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  NEXT STEPS IN OPENEARCH DASHBOARDS                         ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  1. Open: http://localhost:5601                              ║"
echo "║  2. Click: Discover → Select 'logs-*' → See live logs       ║"
echo "║  3. Click: Visualize → Create charts:                       ║"
echo "║     a. Bar: Log Count by Service (field: service)           ║"
echo "║     b. Pie: Error Rate by Service (filter: level:ERROR)     ║"
echo "║     c. Line: Latency Over Time (field: duration_ms)         ║"
echo "║     d. Table: Recent Errors (filter: level:ERROR,FATAL)     ║"
echo "║  4. Click: Dashboard → Add all visualizations               ║"
echo "║  5. Click: Dev Tools → Paste queries from kibana-export.json║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  KEY QUERIES TO RUN IN DEV TOOLS:                           ║"
echo "║  GET /logs-*/_count                (total records)          ║"
echo "║  GET /logs-*/_stats/store          (storage used in bytes)  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Print current stats
info "Current OpenSearch stats:"
count=$(curl -sf "$OS/logs-*/_count" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
echo "  Indexed records: $count"

echo ""
success "Dashboard setup complete!"
