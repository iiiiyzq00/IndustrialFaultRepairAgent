#!/usr/bin/env bash
# =============================================================================
# 将宿主机 kind 集群接入 docker-compose 网络 (ifr-network)
# =============================================================================
# 背景:
#   kind 集群运行在独立的 Docker 网络中 (默认 "kind")。
#   docker-compose 服务运行在 "ifr-network" 网络中。
#   此脚本负责打通两个网络，使容器能够访问 kind API server。
#
# 前置条件:
#   1. kind 集群 "ifr-cluster" 已创建
#   2. docker-compose 服务已启动 (ifr-network 存在)
#
# 原理:
#   kind 的 control-plane 容器已映射 API server 端口到宿主机。
#   容器可通过 host.docker.internal (host-gateway) 访问宿主机端口。
#   此脚本更新 kubeconfig，将 server 地址替换为容器可访问的地址。
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
KUBECTL="${KUBECTL:-$BIN_DIR/kubectl}"
KIND="${KIND:-$BIN_DIR/kind}"
CLUSTER_NAME="${CLUSTER_NAME:-ifr-cluster}"
KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
KUBECONFIG_DOCKER="${KUBECONFIG}.docker"

# ── 1. 检查前置条件 ──────────────────────────────────────────
info "Checking prerequisites..."

# kind cluster
if ! "$KIND" get clusters 2>/dev/null | grep -q "$CLUSTER_NAME"; then
    error "kind cluster '$CLUSTER_NAME' not found. Run: bash scripts/setup_env.sh"
    exit 1
fi
info "kind cluster '$CLUSTER_NAME' found"

# kubectl
if ! "$KUBECTL" version --client &>/dev/null; then
    error "kubectl not found at $KUBECTL"
    exit 1
fi

# ifr-network
if ! docker network inspect ifr-network &>/dev/null; then
    warn "ifr-network not found — docker-compose may not be running"
    warn "Please start services first: docker compose up -d"
fi

# ── 2. 获取 kind API server 端口 ─────────────────────────────
info "Finding kind API server endpoint..."

# kind 将 API server 映射到宿主机的一个随机高端口
# 从 Docker 端口映射中获取
CONTROL_PLANE=$(docker ps --filter "name=ifr-cluster-control" --format "{{.Names}}" | head -1)
if [ -z "$CONTROL_PLANE" ]; then
    error "Cannot find kind control-plane container"
    exit 1
fi
info "Control-plane container: $CONTROL_PLANE"

API_PORT=$(docker port "$CONTROL_PLANE" 6443/tcp 2>/dev/null | head -1 | sed 's/.*://')
if [ -z "$API_PORT" ]; then
    error "Cannot determine API server host port"
    exit 1
fi
info "API server exposed on host port: $API_PORT"

# ── 3. 生成容器可用的 kubeconfig ─────────────────────────────
info "Generating docker-compatible kubeconfig: $KUBECONFIG_DOCKER"

# 复制当前 kubeconfig 并将 server 地址改为 host.docker.internal
# 这样 docker-compose 容器（配置了 extra_hosts）可以访问
"$KUBECTL" config view --raw | \
    sed "s|server: https://127.0.0.1:${API_PORT}|server: https://host.docker.internal:${API_PORT}|g" | \
    sed "s|server: https://.*:6443|server: https://host.docker.internal:${API_PORT}|g" \
    > "$KUBECONFIG_DOCKER"

chmod 600 "$KUBECONFIG_DOCKER"
info "Docker kubeconfig written: $KUBECONFIG_DOCKER"

# ── 4. 检查宿主机 kubeconfig 是否已可用 ─────────────────────
info "Checking if host kubeconfig uses accessible address..."
CURRENT_SERVER=$("$KUBECTL" config view --raw -o jsonpath='{.clusters[?(@.name=="kind-ifr-cluster")].cluster.server}' 2>/dev/null || echo "")

if echo "$CURRENT_SERVER" | grep -q "127.0.0.1\|localhost"; then
    warn "Host kubeconfig uses localhost — updating to 0.0.0.0 for container access..."
    # Also update the host kubeconfig to use 0.0.0.0 (works both on host and containers with host-gateway)
    "$KUBECTL" config set-cluster "kind-${CLUSTER_NAME}" \
        --server="https://0.0.0.0:${API_PORT}" \
        &>/dev/null || warn "Could not update kubeconfig cluster server (non-critical)"
fi

# ── 5. 连接 kind Docker 网络到 ifr-network ──────────────────
info "Connecting kind network to ifr-network..."
KIND_NETWORK="kind"
if docker network inspect "$KIND_NETWORK" &>/dev/null; then
    # Check if already connected
    if docker network inspect ifr-network | grep -q "$KIND_NETWORK"; then
        info "Networks already connected"
    else
        # Connect the kind network to ifr-network by attaching the
        # control-plane container to ifr-network
        if docker network connect ifr-network "$CONTROL_PLANE" 2>/dev/null; then
            info "Connected $CONTROL_PLANE to ifr-network"
        else
            warn "Could not connect control-plane to ifr-network (may already be connected)"
        fi
    fi
else
    warn "kind Docker network '$KIND_NETWORK' not found — skipping network bridge"
fi

# ── 6. 验证 ──────────────────────────────────────────────────
echo ""
echo "========================================="
echo "  kind → Docker 接入状态"
echo "========================================="
echo "  Cluster       : $CLUSTER_NAME"
echo "  API port      : $API_PORT"
echo "  Container URL : https://host.docker.internal:$API_PORT"
echo "  Kubeconfig    : $KUBECONFIG_DOCKER"
echo ""

# Quick connectivity test from host
if "$KUBECTL" cluster-info &>/dev/null; then
    info "kubectl cluster-info: OK"
else
    warn "kubectl cluster-info failed — cluster may be starting"
fi

echo ""
echo "  To use in docker-compose containers, mount:"
echo "    ${KUBECONFIG_DOCKER}:/root/.kube/config:ro"
echo ""
echo "  Or update docker-compose.yml to use:"
echo "    volumes:"
echo "      - \${HOME}/.kube/config.docker:/root/.kube/config:ro"
echo "========================================="
