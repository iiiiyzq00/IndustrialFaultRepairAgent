# 工业故障自愈 Multi-Agent 系统

面向工业异构网络的故障诊断与自愈系统。基于 **Flink 双流异常检测 + Supervisor-Worker 多智能体协同 + 数字孪生沙盒 + RAG 经验召回 + HITL 安全自愈执行 + 复盘经验飞轮**，将工业产线排障响应时间压缩到 **<90 秒**，定界准确率 **≥92%**。

## 系统架构

```
                         ┌──────────────────────────────┐
   Fake Generator ──→  Kafka  ──→  Flink 双流异常检测   │
   (42节点×10指标)    消息队列    动态基线+z-score       │
                    ┌─ metrics ──→ 去抖动+聚合           │
                    ├─ alerts  ──→ 交叉验证 ──┐         │
                    │         Webhook ↓        │         │
                         └──────────────────────────────┘
                                    │
   ┌────────────────────────────────┼──────────────────────────────────┐
   │               Supervisor (LangGraph StateGraph — 7 节点)           │
   │  ┌────────────────────────────────────────────────────────────┐   │
   │  │  rag_prefetch → dispatch_experts → arbitrate              │   │
   │  │       │              │                │                    │   │
   │  │       │    ┌─────────┴─────────┐      │                    │   │
   │  │       │    │ 4 Expert Workers  │      │                    │   │
   │  │       │    │ K8s/MW/NW/App     │      │                    │   │
   │  │       │    └─────────┬─────────┘      │                    │   │
   │  │       │              │                ↓                    │   │
   │  │       │              └────────→  conditional               │   │
   │  │       │                          routing                   │   │
   │  │       │                      risk=low → sandbox           │   │
   │  │       │                      risk≠low → HITL → sandbox    │   │
   │  │       ↓                                                    │   │
   │  │  checkpoint persisted                                       │   │
   │  │  to SQLite at every node    sandbox_verify                 │   │
   │  │  (AsyncSqliteSaver)              ↓                          │   │
   │  │                          execute_and_observe               │   │
   │  │                                   ↓                         │   │
   │  │                               review → END                  │   │
   │  └────────────────────────────────────────────────────────────┘   │
   │                          ↑ ↓                                      │
   │    RAG 管线 (ChromaDB + BM25 + BGE + DeepSeek) + 钉钉/企微通知    │
   │                          ↑ ↓                                      │
   │    HITL 审批 (低自动/中单审/高双审) + 动作执行器 (12 种动作)      │
   │                          ↑ ↓                                      │
   │    观察窗口 (10s采集 + 自动回滚) + 复盘飞轮 (LLM → ChromaDB)     │
   └──────────────────────────────────────────────────────────────────┘

数据底座: HBase 时序库 + Kafka 消息流 + ChromaDB 向量库 + MySQL 关系库 + Redis 缓存
```

## 快速开始

### 前置条件

- Docker ≥ 24.0, Docker Compose ≥ 2.20
- Python 3.11+（本地脚本）

### 1. 配置

```bash
cp .env.example .env
# 编辑 .env: DEEPSEEK_API_KEY=sk-your-key
# 可选: DINGTALK_WEBHOOK_URL=https://oapi.dingtalk.com/robot/send?...
```

### 2. (可选) 安装 K8s 环境

如需真实 K8s 工具调用（kubectl, kind 集群）:

```bash
bash scripts/setup_env.sh
```

### 3. 一键启动

```bash
chmod +x start.sh demo.sh acceptance_test.sh
./start.sh
```

> **重要**：修改 `.env` 或 `docker-compose.yml` 后需 `docker compose up -d --force-recreate` 重建容器。

### 4. 验证

```bash
# E2E 连通性测试
./test_e2e.sh

# 集成测试 (3 个场景)
bash tests/run_full_integration.sh

# LangGraph 持久化专项测试 (8 项)
bash tests/test_langgraph_persistence.sh

# 完整验收测试 (69 项, 10 阶段)
./acceptance_test.sh

# 52 场景基准回归
python3 tests/run_benchmark.py -n 52

# 现场演示
bash demo.sh quick   # 快速演示 (~2 分钟)
bash demo.sh full    # 完整演示 (~5 分钟)
```

### 5. 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| Supervisor | 8100 | 主控 Agent (LangGraph 7 节点), `/metrics` |
| K8s Expert | 8110 | K8s 领域专家, 5 tools |
| Middleware Expert | 8120 | Redis/MySQL/Kafka 专家, 9 tools |
| Network Expert | 8130 | 网络探测专家, 4 tools |
| App Expert | 8140 | APM/日志/HBase 专家, 5 tools |
| RAG Service | 8200 | 混合检索管线 |
| HITL Gateway | 8300 | 审批网关 + WebSocket + 钉钉/企微通知 |
| Action Executor | 8400 | 自愈动作执行器, 12 actions |
| Sandbox | 8500 | 数字孪生沙盒 (动作安全预验证) |
| Prometheus | 9090 | 监控指标采集 (24 metrics) |
| Grafana | 3002 | 监控仪表板 (admin/admin) |
| Redis | (内部) | 真实 Redis 7 实例 (容器间通信) |
| MySQL | (内部) | 真实 MySQL 8.0 (容器间通信) |
| HBase | 9095/16010 | 时序库 (Thrift/Master UI) |
| ChromaDB | 8002 | 向量数据库 |
| Kafka | 9092/9093 | 消息队列 (内部/外部) |
| Flink UI | 8081 | Flink Web 控制台 |
| 审批面板 | 3000 | HITL Web 审批界面 |
| MinIO Console | 9001 | 对象存储管理 |
| K8s Mock | 9002 | K8s 模拟 (10 scenarios) |
| Redis Mock | 9003 | Redis 模拟 (10 scenarios) |
| Network Mock | 9004 | 网络模拟 (12 scenarios) |
| Fake Generator | 9005 | 42 节点 × 10 指标模拟 + 20 故障场景 |

## 核心能力

### LangGraph 诊断管线 (v2.1)

基于 **LangGraph StateGraph** 的 7 节点异步诊断管线，每节点自动 checkpoint：

```
rag_prefetch → dispatch_experts → arbitrate → [conditional]
                                                  ↓
                    ┌─ low-risk ──→ sandbox_verify ──→ execute_and_observe → review → END
                    │                                    ↑
                    └─ mid/high ──→ hitl_interrupt ──────┘
```

- **RAG 预取**: 自然语言查询生成 → ChromaDB+BM25+RRF+BGE+DeepSeek 精排 → top-3 案例
- **专家并行调度**: 4 专家并发诊断，共享 RAG 上下文
- **仲裁决策**: 证据权重投票（含交叉验证）→ 得分差 <20% 降级为对抗辩论
- **条件路由**: `risk=low` → sandbox_verify; `risk≠low` → HITL → sandbox_verify
- **数字孪生沙盒**: 所有动作在沙盒中预模拟 (LLM+RAG+规则引擎)，blocked 动作跳过执行
- **观察窗口**: 60s/90s/120s 按风险分级 + 每 10s 指标采集 + 恶化自动回滚
- **复盘飞轮**: LLM 生成结构化 Markdown 案例 → 向量化 → ChromaDB 入库
- **HITL 中断**: LangGraph `interrupt()` 暂停 graph，`POST /{trace_id}/resume` 恢复
- **状态持久化**: `AsyncSqliteSaver` 在每节点转换时自动 checkpoint 到 SQLite，进程重启后恢复
- **并发隔离**: 每个 `thread_id` 独立 checkpoint 空间

### 流式感知层

- **Flink 1.18** 双流消费 Kafka（`industrial-metrics` + `industrial-alerts`）
- 动态基线（7 天滑动窗口 + RocksDB 状态后端 + Welford 算法）
- 差异化 z-score（延迟 2.5σ / 队列 2.0σ / 资源 3.0σ / 错误率绝对值）
- 连续窗口去抖动（N=3）+ 5 分钟同根因告警聚合
- **告警交叉验证**: 外部告警与指标异常双向确认，交叉命中优先级 ×1.5
- 事件时间处理 + 5 秒水位线乱序容忍

### 多智能体排障

- **Supervisor-Worker** 范式：1 主控 + 4 专家 + 1 仲裁
- **RAG 管线**: ChromaDB 向量检索 + BM25 稀疏检索 + RRF 融合(k=60) + BGE-Reranker-v2-m3 粗筛 + DeepSeek-V3 精排
- **RAG 智能查询**: 自动将告警指标转为自然中文描述（`"Container order-svc 发生 严重 告警，P99 延迟 从 80.0 异常升高至 1200.0"`）
- **仲裁策略**: 证据权重投票（含交叉验证）→ 得分差 <20% 降级为对抗辩论
- **MCP 工具**: 22 个领域工具（K8s 5 + 中间件 9 + 网络 4 + 应用 4），支持 Mock/真实双模切换

### 安全自愈

- **HITL 三级审批**: 低风险自动执行 / 中风险单人审批 / 高风险双人审批
- **12 种自愈动作**: rollback / scale / scale_down / restart / redis_config / mysql_failover / mysql_kill_query / plc_rollback / cnc_adjust / emergency_stop / network_traffic_shift / dns_failover
- **数字孪生沙盒**: 所有动作执行前在沙盒中预验证（模拟器+LLM+RAG 三重评估），blocked 时自动替换为更安全的替代方案
- **观察窗口**: 60s/90s/120s 按风险分级 + 每 10 秒指标采集 + 恶化自动回滚
- **通知**: 钉钉/企微 Webhook（审批事件 + 自愈结果）
- **回滚剧本**: 每个自动下发动作配套自动生成的回滚剧本

### 经验飞轮

- 故障闭环后自动触发复盘 → DeepSeek-V3 提炼核心现象/根因/处置步骤 → 生成标准化 Markdown
- 自动向量化并推送到 ChromaDB，扩充 RAG 语料库
- 200 条种子工单，飞轮自动增长
- 同名故障再次发生时直接命中历史案例

### 可观测性

- 24 个 Prometheus 指标（诊断耗时/置信度/MTTR/RAG 文档数/HITL 审批/动作执行/沙盒验证）
- 每个服务暴露 `/metrics` 端点
- Grafana 仪表板 `dashboards/industrial-aiops.json`
- 钉钉/企微实时通知

## 项目结构

```
.
├── configs/
│   ├── anomaly_scenarios.yaml       # 故障注入场景 (20 个)
│   ├── mock_scenarios/              # Mock 场景 (32 个: K8s 10 + Redis 10 + Network 12)
│   ├── seed_tickets.json            # RAG 种子工单 (200 条)
│   ├── prometheus/prometheus.yml    # Prometheus 采集配置
│   ├── grafana/                     # Grafana provisioning
│   └── mysql/init.sql               # MySQL 初始化
├── flink/jobs/                      # Flink 双流作业 (Java 11, 11 文件)
│   └── src/main/java/com/ifr/anomaly/
│       ├── AnomalyDetectionJob.java # 主作业 (双 Kafka Source)
│       ├── model/                   # MetricEvent, AlertEvent, IncidentEvent
│       ├── process/                 # DynamicBaseline, ZScoreRouter, DeJitterWindow,
│       │                             # AggregationFunction, AlertCrossValidator
│       ├── config/                  # ThresholdConfig
│       └── sink/                    # WebhookSink
├── services/
│   ├── common/
│   │   ├── auth.py                  # API Key 认证
│   │   ├── metrics.py               # Prometheus 指标
│   │   ├── notifier.py              # 钉钉/企微通知
│   │   └── hbase_client.py          # HBase 时序客户端
│   ├── mock-services/               # K8s/Redis/Network Mock
│   ├── fake-data-generator/         # 42 节点 × 10 指标模拟器
│   ├── agent-supervisor/            # 主控 Agent (LangGraph)
│   │   └── app/
│   │       ├── graph.py             # 7 节点管线 + AsyncSqliteSaver
│   │       ├── main.py              # REST API
│   │       ├── arbitrator.py        # 仲裁 (投票+辩论)
│   │       ├── self_healer.py       # 自愈执行 + 观察窗口
│   │       ├── review_extractor.py  # 复盘飞轮
│   │       ├── sandbox_client.py    # 沙盒 HTTP 客户端
│   │       ├── rag_client.py        # RAG HTTP 客户端
│   │       ├── expert_client.py     # 专家 HTTP 客户端
│   │       └── schemas.py           # 数据模型
│   ├── expert-worker/               # 4 专家共用代码基
│   │   └── tools/                   # k8s_tools, middleware_tools, network_tools, app_tools
│   ├── rag-service/                 # RAG 管线
│   │   └── app/
│   │       ├── chroma_client.py     # ChromaDB
│   │       ├── bm25_index.py        # BM25
│   │       ├── rrf.py               # RRF 融合
│   │       └── reranker.py          # BGE + DeepSeek 精排
│   ├── action-executor/             # 自愈动作执行器 (12 actions)
│   │   └── handlers/                # k8s, middleware, industrial, network
│   ├── sandbox-service/             # 数字孪生沙盒
│   │   └── app/
│   │       ├── simulator.py         # 动作效果模拟
│   │       └── main.py              # LLM+RAG+规则 评估
│   ├── hitl-gateway/                # HITL 审批网关
│   └── hitl-frontend/               # Web 审批面板
├── tests/
│   ├── acceptance_test.sh           # 验收测试 (69 项, 10 阶段)
│   ├── run_full_integration.sh      # 集成测试 (3 场景)
│   ├── test_langgraph_persistence.sh # 持久化测试 (8 项)
│   ├── run_benchmark.py             # 52 场景基准测试
│   ├── benchmark_scenarios.py       # 场景编排矩阵
│   └── helpers.py                   # 测试辅助
├── docs/
│   ├── deployment.md                # 部署指南
│   ├── operations.md                # 运维手册
│   ├── tuning.md                    # 性能调优
│   └── fault-injection/README.md    # 故障注入手册 (52 场景)
├── dashboards/industrial-aiops.json # Grafana 仪表板
├── scripts/
│   ├── setup_env.sh                 # 环境初始化 (kind+kubectl+HBase)
│   ├── connect_kind_to_docker.sh    # kind→Docker 网络桥接
│   └── download_models.py           # HF 模型预下载
├── acceptance_test.sh               # 完整验收脚本
├── demo.sh                          # 现场演示
├── start.sh                         # 一键启动
├── test_e2e.sh                      # E2E 连通性测试
└── CHECKLIST.md                     # 交付检查清单
```

## 测试

```bash
# E2E 连通性
./test_e2e.sh

# 集成测试 — 3 个场景
bash tests/run_full_integration.sh
#   Scenario A: 低风险自动自愈 (risk=low, confidence≥0.85, MTTR~41s)
#   Scenario B: HITL 审批 / LLM 智能降级自愈
#   Scenario C: 经验飞轮验证 (RAG score≥0.95)

# LangGraph 持久化 — 8 项
bash tests/test_langgraph_persistence.sh

# 52 场景基准回归
python3 tests/run_benchmark.py -n 52
python3 tests/run_benchmark.py --quick    # 快速: 10 场景

# 完整验收 — 69 项, 10 阶段
./acceptance_test.sh
./acceptance_test.sh --quick              # 快速: 仅连通性
```

## 演示

```bash
bash demo.sh quick   # 快速演示 (~2 分钟, 含 RAG 演示)
bash demo.sh full    # 完整演示 (~5 分钟, 含 HITL + 持久化)
```

## 故障注入

```bash
# 查看所有 52 个可用场景
curl http://localhost:9005/scenarios -H "X-API-Key: dev-key-change-me"          # 20 anomaly
curl http://localhost:9002/scenario -H "X-API-Key: dev-key-change-me"           # 10 k8s mock
curl http://localhost:9003/scenario -H "X-API-Key: dev-key-change-me"           # 10 redis mock
curl http://localhost:9004/scenario -H "X-API-Key: dev-key-change-me"           # 12 network mock

# 激活场景
curl -X POST http://localhost:9005/scenarios/latency_spike/activate -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9003/scenario/slow_query -H "X-API-Key: dev-key-change-me"

# 手动触发诊断
curl -X POST http://localhost:8100/api/v1/incident \
  -H "Content-Type: application/json" -H "X-API-Key: dev-key-change-me" \
  -d '{"incident_id":"manual","trigger_time":"2025-06-15T02:33:05Z",
       "aggregated_alerts":[{...}],"node_id":"order-svc",
       "severity_max":"major","affected_line_profile":"general"}'

# 恢复 HITL 中断的诊断
curl -X POST http://localhost:8100/api/v1/incident/{trace_id}/resume \
  -H "Content-Type: application/json" -H "X-API-Key: dev-key-change-me" \
  -d '{"status":"approved","user_id":"sre-001"}'
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEEPSEEK_API_KEY` | — | **必填**，DeepSeek-V3 API Key |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com/v1` | DeepSeek API 地址 |
| `LLM_MODEL` | `deepseek-chat` | LLM 模型名 |
| `HF_CACHE_DIR` | `~/.cache/huggingface` | HuggingFace 模型缓存 |
| `API_KEY` | `dev-key-change-me` | 服务间 API Key（生产需更换） |
| `DINGTALK_WEBHOOK_URL` | — | 钉钉通知 Webhook（可选） |
| `WECOM_WEBHOOK_URL` | — | 企业微信通知 Webhook（可选） |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `DRY_RUN` | `true` | 动作执行器干跑模式（开发） |
| `CHECKPOINT_DB` | `/app/data/checkpoints.db` | LangGraph checkpoint 路径 |
| `RAG_TIMEOUT_SECONDS` | `15` | RAG 检索超时（秒） |
| `SANDBOX_VERIFY_TIMEOUT_SECONDS` | `10` | 沙盒验证超时（秒） |

## API 端点

除 `/health` 外所有端点需 `X-API-Key: dev-key-change-me`。

### Supervisor (8100)

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 健康检查（含 checkpointer 类型） |
| `POST` | `/api/v1/incident` | Flink Webhook — 创建诊断 |
| `GET` | `/api/v1/diagnosis/{trace_id}` | 查询诊断状态（从 SQLite checkpoint 读取） |
| `GET` | `/api/v1/diagnoses` | 列出活跃诊断 |
| `POST` | `/api/v1/incident/{trace_id}/resume` | HITL 审批后恢复 graph |
| `POST` | `/api/v1/incident/{trace_id}/fallback` | 审批超时降级处理 |

### HITL Gateway (8300)

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/approvals` | 创建审批 |
| `GET` | `/api/v1/approvals/pending` | 列出待审批 |
| `GET` | `/api/v1/approvals/{id}` | 审批详情 |
| `POST` | `/api/v1/approvals/{id}/approve` | 批准（回调 Supervisor） |
| `POST` | `/api/v1/approvals/{id}/reject` | 拒绝（回调 Supervisor） |
| `WS` | `/api/v1/approvals/ws` | WebSocket 实时推送 |

### RAG Service (8200)

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/rag/retrieve` | 混合检索（ChromaDB+BM25+RRF+精排） |
| `POST` | `/api/v1/rag/upsert` | 文档入库（复盘飞轮调用） |

### Sandbox Service (8500)

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/v1/sandbox/verify` | 沙盒验证（模拟器+LLM+RAG） |

## 技术栈

| 层级 | 技术 |
|------|------|
| Multi-Agent 编排 | **LangGraph 1.2.x** (StateGraph + interrupt/resume) |
| 状态持久化 | **AsyncSqliteSaver** (SQLite checkpoint, ormsgpack) |
| 流计算 | Apache Flink 1.18, Kafka 3.6（双流） |
| LLM | DeepSeek-V3 (诊断/仲裁/复盘/精排/沙盒评估) |
| RAG | ChromaDB 0.5.23, BGE-large-zh-v1.5, BM25, RRF, BGE-Reranker-v2-m3 |
| 存储 | ChromaDB (向量), Kafka (消息), MySQL 8.0 (关系), Redis 7 (缓存), HBase (时序), MinIO (对象), SQLite (checkpoint) |
| 监控 | Prometheus (24 指标) + Grafana 仪表板 + 钉钉/企微通知 |
| 安全 | HITL 三级审批 + 自动回滚 + API Key 认证 + 数字孪生沙盒 |
| K8s | kind (开发) / 真实集群 (生产), kubectl, Kubernetes Python Client |
| 运行时 | Python 3.11+, Java 11 (Flink) |

## 核心指标

| 指标 | 目标 | 实测 |
|------|------|------|
| 平均排障响应时间 | < 90s | ~41s |
| 定界置信度 | ≥ 0.85 | 0.85–0.95 |
| RAG 检索精度 | > 0.80 | 0.92–0.95 |
| 低风险自愈成功率 | ≥ 86% | 基准测试验证 |
| RAG 语料库 | 200 种子 | 飞轮自动增长 |
| 故障场景覆盖 | 52 | 20 anomaly + 32 mock |
| Docker 服务 | 22 | 全部可启动 |
| 验收测试 | 69 项 | 全部通过 |

## 文档

- [部署指南](docs/deployment.md)
- [运维手册](docs/operations.md)
- [性能调优](docs/tuning.md)
- [故障注入](docs/fault-injection/README.md)
- [检查清单](CHECKLIST.md)
