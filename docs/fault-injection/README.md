# 故障注入测试套件

## Fake Generator 场景 (20 个)

```bash
# 列出所有场景
curl http://localhost:9005/scenarios -H "X-API-Key: dev-key-change-me"

# 激活/停用
curl -X POST http://localhost:9005/scenarios/<name>/activate -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9005/scenarios/deactivate -H "X-API-Key: dev-key-change-me"
```

### 预定义场景

| 场景 | 描述 |
|------|------|
| `latency_spike` | order-svc P99 延迟 15x 飙升 (KEYS * 阻塞) |
| `oom_cascade` | order-svc OOMKilled 内存泄漏，级联重启风暴 |
| `network_degradation` | 网络丢包导致延迟飙升 + 连接池耗尽 |
| `queue_buildup` | Kafka/Gateway 消息队列深度堆积 |
| `disk_pressure` | K8s 节点磁盘 97% 满 |
| `multi_line_failure` | 跨产线故障: CNC 过热 + PLC 通讯降级 |
| `agv_battery_critical` | AGV 电池电量 12%，信号弱 |
| `connection_storm` | 连接数 10x 飙升，error_rate 急剧上升 |
| `db_connection_exhaustion` | MySQL 连接池耗尽 + 慢查询 |
| `kafka_consumer_lag` | Kafka 消费者 Lag 堆积 + Broker 磁盘压力 |
| `dns_timeout` | DNS 解析超时导致服务间调用失败 |
| `memory_leak_slow` | 渐进式内存泄漏 (start_offset 延迟触发) |
| `cpu_throttling` | CPU 被 K8s limit 限制导致延迟飙升 |
| `plc_comms_timeout` | PLC Modbus 通信超时 |
| `cnc_tool_wear` | CNC 刀具磨损导致精度漂移 |
| `gateway_throttling` | API 网关限流 |
| `config_drift` | 服务间配置版本不一致 |
| `ntp_time_skew` | NTP 时间偏差导致令牌验证失败 |
| `node_disk_full` | 宿主机磁盘满 + IO wait 飙升 |
| `load_balancer_failure` | LB 健康检查失败 + 流量不均衡 |

## Mock 服务场景 (32 个)

```bash
# K8s Mock (10 场景)
curl http://localhost:9002/scenario -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9002/scenario/<name> -H "X-API-Key: dev-key-change-me"

# Redis Mock (10 场景)
curl http://localhost:9003/scenario -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9003/scenario/<name> -H "X-API-Key: dev-key-change-me"

# Network Mock (12 场景)
curl http://localhost:9004/scenario -H "X-API-Key: dev-key-change-me"
curl -X POST http://localhost:9004/scenario/<name> -H "X-API-Key: dev-key-change-me"
```

### K8s 场景

| 场景 | 描述 |
|------|------|
| `default` | 健康集群 |
| `oom` | Pod OOMKilled + BackOff 重启风暴 |
| `crash_loop` | CrashLoopBackOff (NoClassDefFoundError) |
| `crash_loop_backoff` | CrashLoopBackOff (liveness probe 失败) |
| `image_pull_backoff` | ErrImagePull / ImagePullBackOff |
| `node_not_ready` | NodeNotReady (kubelet 停止上报) |
| `pvc_full` | PersistentVolume 98% 满 |
| `hpa_thrashing` | HPA 频繁扩缩容 |
| `stale_endpoints` | Service Endpoints 未更新 |
| `init_container_failure` | Init Container 失败阻塞 Pod 启动 |

### Redis 场景

| 场景 | 描述 |
|------|------|
| `default` | 健康 Redis |
| `slow_query` | KEYS * / HGETALL 慢查询阻塞 |
| `memory_pressure` | 内存 9.8Gi/10Gi, evicted_keys 12000 |
| `connection_storm` | 9850/10000 连接，rejected 340 |
| `maxmemory_eviction` | maxmemory 达到，allkeys-lru 驱逐 |
| `replica_lag` | 主从复制延迟 63210 |
| `sentinel_failover` | Sentinel 主从切换中 |
| `cluster_resharding` | Cluster 槽位迁移 blocking |
| `keyspace_notification_storm` | keyspace 事件通知风暴 |
| `client_connection_storm` | 连接数接近 maxclients |

### Network 场景

| 场景 | 描述 |
|------|------|
| `default` | 健康网络 |
| `packet_loss` | 15% 丢包 order-svc↔Redis |
| `dns_failure` | DNS NXDOMAIN (CoreDNS 故障) |
| `network_partition` | 完全网络隔离 |
| `proxy_timeout` | 反向代理 50s 超时 |
| `firewall_rule_change` | 防火墙规则变更 45% 丢包 |
| `ssl_expiry` | SSL 证书即将过期 |
| `bandwidth_saturation` | 带宽饱和 12% 丢包 |
| `dns_cache_poisoning` | DNS 缓存投毒 |
| `mtu_mismatch` | MTU 不匹配 |
| `tcp_retransmit_storm` | TCP 重传风暴 35% 丢包 |
| `bgp_route_hijack` | BGP 路由劫持 |

## 手动故障注入 (真实环境)

### K8s

```bash
kubectl delete pod order-svc-xxx -n prod      # CrashLoop
kubectl run stress --image=polinux/stress -- stress --cpu 4  # CPU 压力
```

### Redis

```bash
redis-cli -a dev-pass DEBUG SLEEP 2           # 慢查询
redis-cli -a dev-pass CONFIG SET slowlog-log-slower-than 100
```

### MySQL

```sql
SELECT SLEEP(5);                               # 慢查询
STOP SLAVE; SELECT SLEEP(60); START SLAVE;    # 主从延迟
```

## 通过 Supervisor API 触发诊断

```bash
# 低风险 (自动自愈)
curl -X POST http://localhost:8100/api/v1/incident \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-change-me" \
  -d '{"incident_id":"fault-001","trigger_time":"2025-06-15T02:33:05Z","aggregated_alerts":[{"alert_id":"a1","node_id":"order-svc","node_type":"Container","metric_type":"p99_latency_ms","current_value":1200,"baseline_mean":80,"baseline_std":15,"deviation_sigma":5.2,"severity":"major","tags":{"service":"order-svc","version":"v2.4.0"}}],"node_id":"order-svc","metric_group":"latency","severity_max":"major","affected_line_profile":"general"}'

# 高风险 (触发 HITL + 沙盒)
curl -X POST http://localhost:8100/api/v1/incident \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-change-me" \
  -d '{"incident_id":"fault-002","trigger_time":"2025-06-15T04:00:00Z","aggregated_alerts":[{"alert_id":"h1","node_id":"cnc-lathe-03","node_type":"CNC","metric_type":"vibration_mm_s","current_value":12.5,"baseline_mean":2.0,"baseline_std":0.5,"deviation_sigma":8.0,"severity":"critical","tags":{"equipment":"cnc-lathe-03"}}],"node_id":"cnc-lathe-03","metric_group":"resource","severity_max":"critical","affected_line_profile":"general"}'
```

## 观察诊断结果

```bash
# 诊断状态 + 各阶段耗时 (含 7 节点管线)
curl -s http://localhost:8100/api/v1/diagnosis/<trace_id> \
  -H "X-API-Key: dev-key-change-me" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('phase_timings',{}),indent=2))"

# 沙盒验证结果
curl -s http://localhost:8100/api/v1/diagnosis/<trace_id> \
  -H "X-API-Key: dev-key-change-me" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('sandbox_verdict',{}),indent=2))"
```

## 测试用例

| ID | 故障类型 | 风险 | 预期动作 | 预期时间 |
|----|---------|------|---------|---------|
| TC-001 | Redis KEYS * | Low | rollback_deployment | <90s |
| TC-002 | OOMKilled | Low | restart_pod | <90s |
| TC-003 | Network packet_loss | Medium | network_traffic_shift (HITL) | <5min |
| TC-004 | MySQL failover | High | mysql_failover (双审) | <10min |
| TC-005 | Disk full | Medium | restart + cleanup | <90s |
| TC-006 | PLC parameter error | High | plc_parameter_rollback (双审) | <10min |
| TC-007 | CNC overheat | High | emergency_stop (双审+沙盒阻塞) | <5min |
| TC-008 | Connection storm | Low | scale_deployment | <90s |
| TC-009 | DNS failure | Medium | dns_failover (单审) | <5min |
| TC-010 | Kafka lag | Medium | scale_deployment | <90s |
| TC-011 | CPU throttling | Low | scale_deployment | <90s |
| TC-012 | Config drift | Medium | rollback_deployment (单审) | <5min |

## 验证飞轮效果

```bash
# 记录当前 RAG 语料数
curl -s http://localhost:8200/health -H "X-API-Key: dev-key-change-me" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['total_documents'])"

# 运行一次完整诊断
bash demo.sh quick

# 语料数应增加 1
curl -s http://localhost:8200/health -H "X-API-Key: dev-key-change-me" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['total_documents'])"
```

## 自动化测试

```bash
# E2E 连通性 (含 sandbox)
./test_e2e.sh

# 集成测试 (3 场景: 低风险/高风险/飞轮)
bash tests/run_full_integration.sh

# LangGraph 持久化 (8 项)
bash tests/test_langgraph_persistence.sh

# 52 场景基准测试
python3 tests/run_benchmark.py -n 52

# 完整验收 (69 项, 10 阶段)
./acceptance_test.sh

# 快速验收
./acceptance_test.sh --quick
```
