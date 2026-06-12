-- =============================================================================
-- Industrial Fault Repair — MySQL 初始化脚本
-- =============================================================================
-- 此脚本在 MySQL 容器首次启动时自动执行。
-- 创建 industrial_db 数据库和 ifr_app 用户，以及慢查询日志表。
-- =============================================================================

-- 确保数据库存在（MYSQL_DATABASE 环境变量已自动创建）
-- 这里做额外的安全设置

-- 授权 ifr_app 用户对 industrial_db 的全部权限
GRANT ALL PRIVILEGES ON industrial_db.* TO 'ifr_app'@'%';
FLUSH PRIVILEGES;

-- 启用慢查询日志（供 middleware_tools 的 get_mysql_slowlog 使用）
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL slow_query_log_file = '/var/lib/mysql/mysql-slow.log';
SET GLOBAL long_query_time = 0.5;  -- 超过 0.5 秒即为慢查询
SET GLOBAL log_queries_not_using_indexes = 'ON';

USE industrial_db;

-- =============================================================================
-- 故障工单历史表（模拟生产环境数据）
-- =============================================================================
CREATE TABLE IF NOT EXISTS fault_tickets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticket_id VARCHAR(64) NOT NULL UNIQUE,
    node_id VARCHAR(128),
    fault_category VARCHAR(256),
    severity ENUM('minor','warning','major','critical') DEFAULT 'warning',
    root_cause TEXT,
    fix_steps JSON,
    resolved_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_node (node_id),
    INDEX idx_category (fault_category),
    INDEX idx_severity (severity),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 变更记录表（模拟 CI/CD 发布记录）
-- =============================================================================
CREATE TABLE IF NOT EXISTS deployment_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    service_name VARCHAR(128) NOT NULL,
    version VARCHAR(64) NOT NULL,
    deployed_by VARCHAR(64) DEFAULT 'ci-cd',
    changelog TEXT,
    status ENUM('active','superseded','failed','rolled_back') DEFAULT 'active',
    deployed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_service (service_name),
    INDEX idx_deployed (deployed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 自愈动作审计表
-- =============================================================================
CREATE TABLE IF NOT EXISTS action_audit_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    supervisor_trace_id VARCHAR(64) NOT NULL,
    action_type VARCHAR(64) NOT NULL,
    target VARCHAR(256),
    command TEXT,
    status ENUM('pending','approved','executing','success','failed','rollback_triggered','blocked_by_sandbox') DEFAULT 'pending',
    sandbox_verdict ENUM('safe','needs_modification','blocked') NULL,
    duration_ms INT DEFAULT 0,
    error_message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trace (supervisor_trace_id),
    INDEX idx_status (status),
    INDEX idx_executed (executed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 插入测试数据
-- =============================================================================

-- 模拟一些历史部署记录
INSERT INTO deployment_history (service_name, version, changelog, status) VALUES
('order-svc', 'v2.3.0', 'Updated Redis client from Jedis to Lettuce; added connection pooling', 'active'),
('order-svc', 'v2.4.0', 'Refactored cache layer — replaced SCAN with KEYS * for simplicity', 'rolled_back'),
('payment-svc', 'v1.8.2', 'Fixed MySQL connection leak in payment processor', 'active'),
('gateway-svc', 'v3.1.0', 'Upgraded rate limiter algorithm to sliding window', 'active'),
('redis-prod-01', '7.0.11', 'Redis minor version upgrade; no config changes', 'superseded');

-- 模拟故障工单
INSERT INTO fault_tickets (ticket_id, node_id, fault_category, severity, root_cause, fix_steps, resolved_at) VALUES
('INC-20250601-a1b2c3', 'order-svc', 'middleware/redis/performance', 'major',
 '新版本 v2.4.0 误用 KEYS * 全量扫描导致 Redis 阻塞，P99 延迟从 80ms 飙升至 1200ms',
 '["回滚 order-svc 至 v2.3.0", "Redis 禁用 KEYS 命令", "增加 SCAN 游标扫描"]',
 '2025-06-01 03:15:00'),
('INC-20250602-d4e5f6', 'payment-svc', 'middleware/mysql/connection_leak', 'major',
 'MySQL 连接池配置过大导致连接数耗尽，新请求被拒绝',
 '["收紧连接池大小为 20", "kill 长时间 idle 连接", "重启 payment-svc"]',
 '2025-06-02 14:20:00'),
('INC-20250605-g7h8i9', 'k8s-node-03', 'k8s/oom', 'critical',
 'order-svc 内存泄漏导致 Pod OOMKilled，触发级联重启风暴',
 '["水平扩容 order-svc 至 5 副本", "设置 memory limit=2Gi", "重启问题 Pod"]',
 '2025-06-05 08:45:00');
