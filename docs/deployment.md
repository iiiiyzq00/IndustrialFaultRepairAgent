# 部署指南

## 硬件要求

| 组件 | 最低 | 推荐 |
|------|------|------|
| CPU | 8 cores | 16+ cores |
| RAM | 32 GB | 64 GB |
| GPU | — | NVIDIA GPU (8GB+ VRAM) 用于 RAG 模型加速 |
| 磁盘 | 100 GB SSD | 200 GB SSD |
| 网络 | 可访问 api.deepseek.com | 可访问 hf-mirror.com (模型下载) |

## 依赖安装

```bash
# Docker & Docker Compose
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# NVIDIA Container Toolkit (GPU 可选)
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
sudo apt update && sudo apt install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker && sudo systemctl restart docker

# kind + kubectl (如需真实 K8s 工具调用)
bash scripts/setup_env.sh
```

## Python 版本要求

本项目的 Supervisor 使用 LangGraph 1.2.x，**必须 Python 3.11+**。Docker 镜像已使用 `python:3.11-slim` 基础镜像。

## 快速启动

```bash
# 1. 配置 API Key
cp .env.example .env
# 编辑 .env: DEEPSEEK_API_KEY=sk-your-key
# 可选: DINGTALK_WEBHOOK_URL=https://oapi.dingtalk.com/robot/send?...

# 2. (可选) 安装 K8s 环境
bash scripts/setup_env.sh

# 3. 启动全部 22 个服务
chmod +x start.sh demo.sh acceptance_test.sh
docker compose up -d

# 4. 健康检查
./test_e2e.sh

# 5. 打开审批面板
open http://localhost:3000

# 6. 打开 Grafana 仪表板
open http://localhost:3002  # admin/admin
```

> **重要**：如果修改 `.env` 或 `docker-compose.yml` 中的环境变量，必须 `docker compose up -d --force-recreate` 重建容器才能生效。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEEPSEEK_API_KEY` | — | **必填**，DeepSeek API Key |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com/v1` | DeepSeek API 地址 |
| `LLM_MODEL` | `deepseek-chat` | LLM 模型名 |
| `HF_CACHE_DIR` | `~/.cache/huggingface` | 模型缓存目录 |
| `API_KEY` | `dev-key-change-me` | 服务间 API Key（生产需更换） |
| `DINGTALK_WEBHOOK_URL` | — | 钉钉通知 Webhook（可选） |
| `WECOM_WEBHOOK_URL` | — | 企业微信通知 Webhook（可选） |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `DRY_RUN` | `true` | 动作执行器干跑模式（开发） |
| `CHECKPOINT_DB` | `/app/data/checkpoints.db` | LangGraph checkpoint 路径 |
| `RAG_TIMEOUT_SECONDS` | `15` | RAG 检索超时（秒） |
| `SANDBOX_VERIFY_TIMEOUT_SECONDS` | `10` | 沙盒验证超时（秒） |

## 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| Supervisor | 8100 | 主控 Agent (LangGraph 7 节点 + AsyncSqliteSaver) |
| K8s Expert | 8110 | K8s 领域专家, 5 tools |
| Middleware Expert | 8120 | Redis/MySQL/Kafka 专家, 9 tools |
| Network Expert | 8130 | 网络探测专家, 4 tools |
| App Expert | 8140 | APM/日志/HBase 专家, 5 tools |
| RAG Service | 8200 | 混合检索管线 (ChromaDB+BM25+RRF+BGE+LLM) |
| HITL Gateway | 8300 | 审批网关 (APScheduler+WebSocket+通知) |
| Action Executor | 8400 | 自愈动作执行器, 12 actions |
| Sandbox | 8500 | 数字孪生沙盒 (动作安全预验证) |
| Prometheus | 9090 | 监控指标采集 |
| Grafana | 3002 | 监控仪表板 (admin/admin) |
| Redis | (内部) | 真实 Redis 7 实例 |
| MySQL | (内部) | 真实 MySQL 8.0 实例 |
| HBase | 9095/16010 | 时序库 (Thrift API / Master Web UI) |
| ChromaDB | 8002 | 向量数据库 |
| Kafka | 9092/9093 | 消息队列 (内部/外部) |
| Flink UI | 8081 | Flink Web 控制台 |
| 审批面板 | 3000 | HITL Web 审批界面 |
| MinIO Console | 9001 | 对象存储管理 |
| K8s Mock | 9002 | K8s API 模拟 (10 scenarios) |
| Redis Mock | 9003 | Redis API 模拟 (10 scenarios) |
| Network Mock | 9004 | 网络 API 模拟 (12 scenarios) |
| Fake Generator | 9005 | 42 节点 × 10 指标模拟 + 20 故障场景 |

## 数据持久化

| 数据 | 位置 | 说明 |
|------|------|------|
| LangGraph Checkpoints | `services/agent-supervisor/data/checkpoints.db` | 卷挂载，进程重启后保留 |
| ChromaDB 向量库 | Docker volume: `chroma_data` | BGE 嵌入向量 + 文档 |
| Redis 数据 | Docker volume: `redis_data` | Redis 快照 |
| MySQL 数据 | Docker volume: `mysql_data` | 故障工单、部署记录、审计日志 |
| HBase 数据 | Docker volume: `hbase_data` | 时序指标归档 |
| Kafka 消息 | Docker volume: `kafka_data` | 消息队列持久化 |
| Prometheus 数据 | Docker volume: `prometheus_data` | 7 天监控数据 |
| Grafana 数据 | Docker volume: `grafana_data` | 仪表板配置 |
| MinIO 对象 | Docker volume: `minio_data` | 对象存储 |

## 停止与清理

```bash
docker compose down           # 停止服务（保留数据卷）
docker compose down -v        # 停止并删除所有数据卷
```

## 常见问题

| 问题 | 解决 |
|------|------|
| DeepSeek API 401 | 检查 `.env` 中 `DEEPSEEK_API_KEY`，更新后需 `--force-recreate` |
| RAG 检索超时 | 增加 `RAG_TIMEOUT_SECONDS`（默认 15s） |
| Supervisor 429 | 同一 node_id 已有活跃诊断，等待完成或换 node_id |
| uvicorn 频繁 reload | 正常（开发模式）。生产环境去掉 `--reload` |
| Python 3.10 错误 | 升级到 Python 3.11+（LangGraph async interrupt 必需） |
| Redis 连接失败 | `docker compose restart redis` |
| MySQL 连接失败 | `docker compose restart mysql`（首次启动需 30s 初始化） |
| HBase 不可用 | `docker compose restart hbase`（首次启动需 60s） |
| Prometheus 无数据 | 确认所有服务 `/metrics` 端点可访问 |
| Grafana 无仪表板 | 检查 `configs/grafana/` provisioning 配置 |
