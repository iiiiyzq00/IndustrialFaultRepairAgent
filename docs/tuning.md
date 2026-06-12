# 性能调优指南

## Flink 并行度和状态

```yaml
# docker-compose.yml 调整
flink-taskmanager:
  environment:
    taskmanager.numberOfTaskSlots: 8
    taskmanager.memory.process.size: 8192m

# 或通过 Flink REST API:
curl -X PATCH http://localhost:8081/jobs/<job-id> \
  -H "Content-Type: application/json" \
  -d '{"parallelism": 4}'
```

**建议:**
- 50 节点以下: parallelism=2, Memory=4096m
- 50-200 节点: parallelism=4, Memory=8192m
- 200+ 节点: parallelism=8, Memory=16384m

**双流优化:**
- 告警流 (`industrial-alerts`) 数据量远小于指标流，可单独设置 `max.poll.records=200`
- 交叉验证窗口 (`CROSS_WINDOW_MS=60000`) 可根据告警延迟调整

## RAG 检索优化

### 检索权重

```bash
# docker-compose.yml 环境变量 (rag-service)
VECTOR_WEIGHT: "0.7"   # 向量检索权重
BM25_WEIGHT:   "0.3"   # BM25 权重
RRF_K:         "60"    # RRF 平滑因子
```

### 检索超时

```
RAG_TIMEOUT_SECONDS: "15"   # 含 DeepSeek 精排延迟
                            # 低于 10s 容易超时
                            # 生产可设 20s
```

### 查询质量

- **种子数据覆盖度**: 200 条种子工单覆盖 16 个故障类别
- **飞轮积累**: 每次诊断完成后自动入库，同类故障命中率提高
- **查询语义**: BGE-large-zh-v1.5 嵌入，自然中文描述格式

## 观察窗口和回滚阈值

| 风险 | 窗口 | 触发阈值 | 连续违规 |
|------|------|---------|---------|
| 低 | 60s | 200% increase | 2 |
| 中 | 90s | 150% increase | 3 |
| 高 | 120s | 100% increase | 2 |

**调优:**
- 误回滚过多: 增加 `consecutive_violations` 或 `threshold_value`
- 回滚不够及时: 减少 `consecutive_violations`
- 观察时间过长: 减少 `watch_window_seconds`

## 专家工具超时

```bash
# docker-compose.yml (agent-supervisor)
EXPERT_TIMEOUT_SECONDS: "25"   # 专家诊断超时
RAG_TIMEOUT_SECONDS: "15"      # RAG 检索超时
SANDBOX_VERIFY_TIMEOUT_SECONDS: "10"  # 沙盒验证超时
```

**建议:**
- 真实 K8s API: timeout=10s
- Redis/MySQL 本地: timeout=3s
- Kafka AdminClient: timeout=5s
- 网络 ping/traceroute: timeout=5s
- RAG 检索: ≥15s（含 DeepSeek 精排）
- 沙盒验证: 10s（含 LLM 调用）

## LLM 配置

```bash
LLM_MODEL: "deepseek-chat"  # 或 deepseek-reasoner
temperature: 0.3            # 降低提高一致性
max_tokens: 2000            # 诊断报告长度
```

## 沙盒验证优化

```bash
# docker-compose.yml (sandbox-service)
SANDBOX_TIMEOUT_SECONDS: "10"   # 沙盒总超时 (LLM+RAG+模拟)
```

- LLM 不可用时降级为纯规则判断
- RAG 不可用时跳过历史案例对比
- 沙盒完全不可用时 fail-open（安全默认通过）

## LangGraph Checkpoint

```bash
CHECKPOINT_DB: /app/data/checkpoints.db

# 维护
sqlite3 services/agent-supervisor/data/checkpoints.db "VACUUM;"
sqlite3 services/agent-supervisor/data/checkpoints.db "PRAGMA optimize;"
```

- AsyncSqliteSaver 每节点转换自动写入 (~10KB/次)
- 每个诊断管线产生 7-10 个 checkpoint
- 定期 VACUUM 回收空间

## 容器资源建议

| 容器 | CPU | 内存 | 说明 |
|------|-----|------|------|
| agent-supervisor | 2 cores | 2 GB | LangGraph + LLM 调用 |
| rag-service | 4 cores | 8 GB | BGE 模型 + 精排 |
| sandbox-service | 1 core | 1 GB | LLM + 模拟器 |
| chromadb | 1 core | 2 GB | 向量检索 |
| redis | 0.5 cores | 512 MB | 缓存 |
| mysql | 1 core | 2 GB | 关系数据 |
| hbase | 2 cores | 2 GB | 时序存储 |
| prometheus | 1 core | 2 GB | 7 天监控数据 |
| grafana | 0.5 cores | 512 MB | 仪表板 |
| flink-taskmanager | 2 cores | 4 GB | 双流计算 |
| k8s-expert | 0.5 cores | 512 MB | 轻量 |
| middleware-expert | 0.5 cores | 512 MB | Redis/MySQL/Kafka |
| network-expert | 0.5 cores | 512 MB | 轻量 |
| app-expert | 0.5 cores | 512 MB | 轻量 |

## Flink 检查点

```bash
env.enableCheckpointing(60_000)  # 60s (生产建议 30s)
```

## HBase 优化

```bash
# happybase 连接池
HBASE_HOST: hbase
HBASE_PORT: "9095"    # Thrift API
```

- 指标写入批量大小: 每 60s 一批
- 行键设计: `{node_id}:{metric_type}:{minute_bucket}` 优化范围扫描
- TTL: 7 天 (168 versions)
