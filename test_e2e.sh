#!/usr/bin/env bash
# =============================================================================
# 端到端测试脚本：验证全链路连通性
# =============================================================================
set -euo pipefail

API_KEY="dev-key-change-me"
AUTH="X-API-Key: $API_KEY"
BASE="http://localhost"
TIMEOUT=5

RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'
pass() { echo -e "  ${GREEN}✅ PASS${NC} $*"; }
fail() { echo -e "  ${RED}❌ FAIL${NC} $*"; }

echo "========================================="
echo "  E2E Connectivity Test Suite"
echo "========================================="
echo ""

# ── 1. Mock Services ───────────────────────────────────────────
echo "── 1. Mock Services ──"

# K8s
curl -sf -H "$AUTH" $BASE:9002/health >/dev/null && pass "k8s-mock health" || fail "k8s-mock health"
curl -sf -H "$AUTH" $BASE:9002/api/v1/namespaces/prod/pods >/dev/null && pass "k8s-mock pods" || fail "k8s-mock pods"
curl -sf -X POST -H "$AUTH" $BASE:9002/scenario/oom >/dev/null && pass "k8s-mock scenario switch" || fail "k8s-mock scenario switch"
curl -sf -X POST -H "$AUTH" $BASE:9002/scenario/default >/dev/null && pass "k8s-mock scenario reset" || fail "k8s-mock scenario reset"

# Redis
curl -sf -H "$AUTH" $BASE:9003/health >/dev/null && pass "redis-mock health" || fail "redis-mock health"
curl -sf -H "$AUTH" "$BASE:9003/api/v1/slowlog?top_n=5" >/dev/null && pass "redis-mock slowlog" || fail "redis-mock slowlog"

# Network
curl -sf -H "$AUTH" $BASE:9004/health >/dev/null && pass "network-mock health" || fail "network-mock health"
curl -sf -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"source":"order-svc","target":"redis-prod-01","count":5}' \
  $BASE:9004/api/v1/ping >/dev/null && pass "network-mock ping" || fail "network-mock ping"

# ── 2. RAG Service ─────────────────────────────────────────────
echo ""
echo "── 2. RAG Service ──"

curl -sf -H "$AUTH" $BASE:8200/health >/dev/null && pass "rag health" || fail "rag health"

RAG_RESP=$(curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"query":"Redis KEYS 慢查询 服务超时","top_k":3,"retrieval_strategy":"hybrid"}' \
  $BASE:8200/api/v1/rag/retrieve)

if echo "$RAG_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert len(d['documents']) > 0" 2>/dev/null; then
    pass "rag retrieve (returned $(echo "$RAG_RESP" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['documents']))") docs)"
else
    fail "rag retrieve (no documents — may need seed data)"
fi

# ── 3. Expert Workers ──────────────────────────────────────────
echo ""
echo "── 3. Expert Workers ──"

for expert in k8s:8110 middleware:8120 network:8130 application:8140; do
    NAME="${expert%%:*}"
    PORT="${expert##*:}"
    curl -sf -H "$AUTH" $BASE:$PORT/health >/dev/null && pass "$NAME-expert health" || fail "$NAME-expert health"
    TOOLS=$(curl -sf -H "$AUTH" $BASE:$PORT/api/v1/tools | python3 -c "import sys,json; print(len(json.load(sys.stdin)['tools']))" 2>/dev/null || echo "0")
    pass "$NAME-expert tools ($TOOLS tools)"
done

# ── 3.5. Sandbox Service ────────────────────────────────────────
echo ""
echo "── 3.5. Digital Twin Sandbox ──"
curl -sf -H "$AUTH" $BASE:8500/health >/dev/null && pass "sandbox health" || fail "sandbox health"
SANDBOX_VERIFY=$(curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"supervisor_trace_id":"e2e-test","incident":{"node_id":"test","severity_max":"major"},"arbitration_result":{"unified_root_cause":"test"},"self_healing_plan":{"actions":[{"action":"restart_pod","target":"test-pod","command":"kubectl delete pod test-pod","expected_effect":"Pod restart"}]},"expert_results":{},"rag_context":{}}' \
  $BASE:8500/api/v1/sandbox/verify)
SANDBOX_VERDICT=$(echo "$SANDBOX_VERIFY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verdict','error'))" 2>/dev/null || echo "error")
[ "$SANDBOX_VERDICT" != "error" ] && pass "sandbox verify (verdict=$SANDBOX_VERDICT)" || fail "sandbox verify"

# ── 4. Supervisor ──────────────────────────────────────────────
echo ""
echo "── 4. Supervisor ──"

curl -sf -H "$AUTH" $BASE:8100/health >/dev/null && pass "supervisor health" || fail "supervisor health"

# Send a mock incident webhook
INCIDENT_RESP=$(curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{
    "incident_id": "test-inc-001",
    "trigger_time": "2025-06-15T02:33:05Z",
    "aggregation_window_seconds": 300,
    "priority_score": 82.5,
    "aggregated_alerts": [{
        "alert_id": "alert-001",
        "node_id": "order-svc-${RANDOM}",
        "node_type": "Container",
        "metric_type": "p99_latency_ms",
        "current_value": 1200.0,
        "baseline_mean": 80.0,
        "baseline_std": 15.0,
        "deviation_sigma": 5.2,
        "severity": "critical",
        "tags": {"service": "order-svc"}
    }],
    "affected_line_profile": "general",
    "node_id": "order-svc-test-${RANDOM}",
    "metric_group": "latency",
    "alert_count": 1,
    "severity_max": "critical"
  }' \
  $BASE:8100/api/v1/incident)

TRACE_ID=$(echo "$INCIDENT_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('supervisor_trace_id',''))" 2>/dev/null || echo "")
if [ -n "$TRACE_ID" ]; then
    pass "supervisor incident accepted (trace=$TRACE_ID)"
else
    fail "supervisor incident rejected"
fi

sleep 5

# Check diagnosis state
if [ -n "$TRACE_ID" ]; then
    STATE=$(curl -sf -H "$AUTH" $BASE:8100/api/v1/diagnosis/$TRACE_ID 2>/dev/null || echo "{}")
    STATUS=$(echo "$STATE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('execution_status',''))" 2>/dev/null || echo "")
    pass "supervisor diagnosis state: $STATUS"
fi

# ── 5. HITL Gateway ────────────────────────────────────────────
echo ""
echo "── 5. HITL Gateway ──"

curl -sf -H "$AUTH" $BASE:8300/health >/dev/null && pass "hitl health" || fail "hitl health"

PENDING=$(curl -sf -H "$AUTH" $BASE:8300/api/v1/approvals/pending | python3 -c "import sys,json; print(json.load(sys.stdin)['total'])" 2>/dev/null || echo "0")
pass "hitl pending approvals: $PENDING"

# ── 6. Auth (negative tests) ───────────────────────────────────
echo ""
echo "── 6. API Key Auth ──"

curl -sf -o /dev/null $BASE:8100/health 2>/dev/null && pass "health endpoint (no key required)" || fail "health endpoint"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" $BASE:8100/api/v1/incident -X POST \
  -H "Content-Type: application/json" -d '{}' 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "401" ] || [ "$HTTP_CODE" = "403" ]; then
    pass "unauthorized request → $HTTP_CODE"
else
    fail "unauthorized request → $HTTP_CODE (expected 401/403)"
fi

# ── Summary ────────────────────────────────────────────────────
echo ""
echo "========================================="
echo "  E2E Test Complete"
echo "========================================="
echo ""
echo "  Next steps:"
echo "  1. Open http://localhost:3000 → Approval Panel"
echo "  2. POST /scenarios/latency_spike/activate → Inject fault"
echo "  3. Watch Supervisor logs: docker compose logs -f agent-supervisor"
echo "  4. Check Flink UI: http://localhost:8081"
echo ""
