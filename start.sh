#!/usr/bin/env bash
# =============================================================================
# 工业故障自愈 Multi-Agent 系统 — 一键启动脚本
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Prerequisites ──────────────────────────────────────────────
if ! command -v docker &>/dev/null; then error "Docker not found"; exit 1; fi
if ! docker compose version &>/dev/null; then error "docker compose not found"; exit 1; fi

if [ ! -f .env ]; then
    warn ".env file not found — creating template..."
    cat > .env <<'EOF'
# DeepSeek API Key (required for LLM features)
DEEPSEEK_API_KEY=sk-your-deepseek-api-key

# DingTalk webhook (optional, for on-call notifications)
# DINGTALK_WEBHOOK_URL=https://oapi.dingtalk.com/robot/send?access_token=xxx
EOF
    warn "Please edit .env with your actual API keys, then re-run start.sh"
    exit 0
fi

# ── Create required directories ────────────────────────────────
mkdir -p flink/jobs/target

# ── Start services in dependency order ─────────────────────────
info "Starting infrastructure layer..."
docker compose up -d zookeeper
sleep 5
docker compose up -d kafka
sleep 5
docker compose up -d chromadb minio
sleep 3

info "Starting mock services..."
docker compose up -d k8s-mock-api redis-mock-api network-mock-api
sleep 3

info "Starting RAG service (this may take a minute for model download)..."
docker compose up -d rag-service
sleep 5

info "Starting Agent services..."
docker compose up -d agent-supervisor
sleep 2
docker compose up -d k8s-expert middleware-expert network-expert app-expert
sleep 2

info "Starting HITL layer..."
docker compose up -d hitl-gateway hitl-frontend

info "Starting Fake Data Generator..."
docker compose up -d fake-data-generator

# ── Flink (optional — requires pre-built JAR) ──────────────────
if [ -f flink/jobs/target/industrial-fault-detection-1.0.0.jar ]; then
    info "Starting Flink cluster..."
    docker compose up -d flink-jobmanager flink-taskmanager
else
    warn "Flink JAR not found (skipping Flink). Build with: cd flink/jobs && mvn package -DskipTests"
fi

# ── Wait & verify ──────────────────────────────────────────────
info "Waiting for services to stabilise (15s)..."
sleep 15

info "Verifying health endpoints..."
declare -A ENDPOINTS=(
    ["chromadb"]="http://localhost:8002/api/v1/heartbeat"
    ["k8s-mock"]="http://localhost:9002/health"
    ["redis-mock"]="http://localhost:9003/health"
    ["network-mock"]="http://localhost:9004/health"
    ["rag"]="http://localhost:8200/health"
    ["supervisor"]="http://localhost:8100/health"
    ["k8s-expert"]="http://localhost:8110/health"
    ["middleware-expert"]="http://localhost:8120/health"
    ["network-expert"]="http://localhost:8130/health"
    ["app-expert"]="http://localhost:8140/health"
    ["hitl-gateway"]="http://localhost:8300/health"
)

PASS=0; FAIL=0
for svc in "${!ENDPOINTS[@]}"; do
    url="${ENDPOINTS[$svc]}"
    if curl -sf -o /dev/null "$url"; then
        echo "  ✅ $svc"
        ((PASS++))
    else
        echo "  ❌ $svc ($url)"
        ((FAIL++))
    fi
done

echo ""
info "Health check: ${PASS} passed, ${FAIL} failed"

# ── Summary ────────────────────────────────────────────────────
echo ""
echo "========================================="
echo "  🏭 Industrial Fault Repair System"
echo "========================================="
echo ""
echo "  Services:"
echo "    ChromaDB         → http://localhost:8002"
echo "    K8s Mock         → http://localhost:9002"
echo "    Redis Mock       → http://localhost:9003"
echo "    Network Mock     → http://localhost:9004"
echo "    Fake Generator   → http://localhost:9005"
echo "    Supervisor       → http://localhost:8100"
echo "    K8s Expert       → http://localhost:8110"
echo "    Middleware Expert→ http://localhost:8120"
echo "    Network Expert   → http://localhost:8130"
echo "    App Expert       → http://localhost:8140"
echo "    RAG Service      → http://localhost:8200"
echo "    HITL Gateway     → http://localhost:8300"
echo "    Approval Panel   → http://localhost:3000"
echo "    MinIO Console    → http://localhost:9001"
echo ""
echo "  Quick test:"
echo "    curl -X POST http://localhost:9005/scenarios/latency_spike/activate"
echo "      -H 'X-API-Key: dev-key-change-me'"
echo ""
echo "  Stop:  docker compose down"
echo "  Reset: docker compose down -v"
echo "========================================="
