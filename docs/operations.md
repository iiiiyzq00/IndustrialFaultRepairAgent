# 运维手册

## Prometheus 指标

所有服务暴露 `/metrics` 端点。关键指标：

| 指标 | 说明 | 告警阈值 |
|------|------|---------|
| `ifr_diagnosis_duration_seconds` | 诊断耗时 | P99 > 120s |
| `ifr_diagnosis_confidence` | 定界置信度 | 平均 < 0.7 |
| `ifr_active_diagnoses` | 活跃诊断数 | > 10 |
| `ifr_rag_document_count` | RAG 语料数 | < 50 |
| `ifr_hitl_approval_total{status="expired"}` | 审批超时 | > 0 |
| `ifr_action_execution_total{status="failed"}` | 动作执行失败 | > 0 |
| `ifr_rag_retrieval_duration_seconds` | RAG 检索延迟 | P99 > 15s |
| `ifr_sandbox_verification_duration_seconds` | 沙盒验证延迟 | P99 > 10s |

## 日志查看

```bash
# 所有服务
docker compose logs -f

# 特定服务
docker compose logs -f agent-supervisor    # LangGraph 管线
docker compose logs -f middleware-expert   # Redis/MySQL/Kafka 工具调用
docker compose logs -f rag-service        # RAG 检索
docker compose logs -f hitl-gateway       # 审批流转 + 通知
docker compose logs -f sandbox-service    # 沙盒验证
docker compose logs -f action-executor    # 自愈动作执行
docker compose logs -f prometheus         # 指标采集

# 查看特定 trace 的完整管线 (含 7 节点)
docker compose logs agent-supervisor | grep "<trace_id>"

# Flink 作业 (含双流消费)
docker logs ifr-flink-tm 2>&1 | grep -E "(ERROR|Webhook|Alert|cross-validated)"

# 查看最近诊断状态 (从 SQLite checkpoint)
curl -s http://localhost:8100/api/v1/diagnosis/<trace_id> \
  -H "X-API-Key: dev-key-change-me" | python3 -m json.tool
```

## 常见故障排查

| 问题 | 可能原因 | 解决 |
|------|----------|------|
| Supervisor 429 | 同一节点已有活跃诊断 | 等待完成或使用不同 node_id |
| RAG 返回 0 条 | timeout 太短 / ChronaDB 未就绪 | 增加 `RAG_TIMEOUT_SECONDS` (≥15)；`docker compose restart chromadb rag-service` |
| Webhook 401 | API Key 错 | 确认 `X-API-Key: dev-key-change-me` |
| Flink Job 失败 | Kafka 未连接 | `docker compose restart kafka` |
| Expert 工具超时 | Mock 服务未响应 / 真实中间件不可达 | 检查 mock:9002-9004 和 redis/mysql (内部)/kafka:9092 |
| LangGraph 中断未恢复 | HITL Gateway 回调失败 | 检查 `hitl-gateway` 日志，手动 `/resume` |
| checkpoints.db 过大 | 历史诊断积累 | `sqlite3 ... "VACUUM;"` 或定期轮转 |
| 沙盒阻塞频繁 | 高风险动作 + 规则引擎触发 | 检查 `sandbox-service` 日志，调整阈值 |
| HBase 写入失败 | happybase 未安装或 HBase 未就绪 | `docker compose restart hbase` |
| Prometheus target down | 服务未暴露 `/metrics` 或 `/health` | 检查对应服务日志 |

## 手动触发诊断

```bash
curl -X POST http://localhost:8100/api/v1/incident \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-change-me" \
  -d '{
    "incident_id":"manual-001",
    "trigger_time":"2025-06-15T02:33:05Z",
    "aggregated_alerts":[{
      "alert_id":"a1","node_id":"order-svc","node_type":"Container",
      "metric_type":"p99_latency_ms","current_value":1200,
      "baseline_mean":80,"baseline_std":15,"deviation_sigma":5.2,
      "severity":"major","tags":{"service":"order-svc"}
    }],
    "node_id":"order-svc","metric_group":"latency",
    "severity_max":"major","affected_line_profile":"general"
  }'
```

## 手动恢复 HITL 中断诊断

```bash
# 查看诊断状态
curl http://localhost:8100/api/v1/diagnosis/<trace_id> -H "X-API-Key: dev-key-change-me"

# 如 awaiting_approval，手动恢复
curl -X POST http://localhost:8100/api/v1/incident/<trace_id>/resume \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-change-me" \
  -d '{"status":"approved","user_id":"admin","reason":"手动恢复"}'
```

## 故障场景注入

```bash
# Fake Generator (20 场景)
curl http://localhost:9005/scenarios -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9005/scenarios/latency_spike/activate -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9005/scenarios/deactivate -H "X-API-Key: dev-key-change-me"

# Mock Services (32 场景)
curl http://localhost:9002/scenario -H "X-API-Key: dev-key-change-me"  # K8s (10)
curl http://localhost:9003/scenario -H "X-API-Key: dev-key-change-me"  # Redis (10)
curl http://localhost:9004/scenario -H "X-API-Key: dev-key-change-me"  # Network (12)
```

## 数据备份

```bash
# LangGraph Checkpoints (SQLite)
cp services/agent-supervisor/data/checkpoints.db backup/checkpoints-$(date +%Y%m%d).db

# ChromaDB
docker run --rm -v chroma_data:/data -v $(pwd)/backup:/backup \
  alpine tar czf /backup/chroma-$(date +%Y%m%d).tar.gz -C /data .

# MySQL
docker exec ifr-mysql mysqldump -u root -pdev-pass industrial_db > backup/mysql-$(date +%Y%m%d).sql

# Prometheus
curl -X POST http://localhost:9090/api/v1/admin/tsdb/snapshot

# Grafana
docker run --rm -v grafana_data:/data -v $(pwd)/backup:/backup \
  alpine tar czf /backup/grafana-$(date +%Y%m%d).tar.gz -C /data .
```

## 审批面板

访问 `http://localhost:3000` 查看待审批的自愈动作。审批后 HITL Gateway 自动回调 Supervisor `/resume` 恢复 LangGraph 管线，同时发送钉钉/企微通知。

## 检查点数据库维护

```bash
# 大小
du -h services/agent-supervisor/data/checkpoints.db

# 检查点数量
sqlite3 services/agent-supervisor/data/checkpoints.db "SELECT COUNT(*) FROM checkpoints"

# 空间回收
sqlite3 services/agent-supervisor/data/checkpoints.db "VACUUM;"
sqlite3 services/agent-supervisor/data/checkpoints.db "PRAGMA optimize;"

# 重置（慎用）
# rm services/agent-supervisor/data/checkpoints.db && docker compose restart agent-supervisor
```
