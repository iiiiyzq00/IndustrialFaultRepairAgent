#!/usr/bin/env bash
# =============================================================================
# 生产级环境依赖安装 — 精简版
# 宿主机已有 Redis(6379) + MySQL(3306)，只需安装 kind + kubectl
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }

BIN_DIR="$HOME/.local/bin"
mkdir -p "$BIN_DIR"

# ── 1. kubectl (local install, no sudo) ────────────────────────
if "$BIN_DIR/kubectl" version --client &>/dev/null; then
    info "kubectl already installed in $BIN_DIR"
else
    info "Installing kubectl v1.27.3..."
    curl -sLo /tmp/kubectl "https://dl.k8s.io/release/v1.27.3/bin/linux/amd64/kubectl"
    chmod +x /tmp/kubectl
    mv /tmp/kubectl "$BIN_DIR/kubectl"
    info "kubectl installed to $BIN_DIR/kubectl"
fi

# ── 2. kind (local install, no sudo) ───────────────────────────
if "$BIN_DIR/kind" version &>/dev/null; then
    info "kind already installed in $BIN_DIR"
else
    info "Installing kind v0.20.0..."
    curl -sLo /tmp/kind "https://github.com/kubernetes-sigs/kind/releases/download/v0.20.0/kind-linux-amd64"
    chmod +x /tmp/kind
    mv /tmp/kind "$BIN_DIR/kind"
    info "kind installed to $BIN_DIR/kind"
fi

# Ensure PATH includes ~/.local/bin
if ! echo "$PATH" | grep -q "$BIN_DIR"; then
    echo "export PATH=\"$BIN_DIR:\$PATH\"" >> "$HOME/.bashrc"
    export PATH="$BIN_DIR:$PATH"
    warn "Added $BIN_DIR to PATH in ~/.bashrc"
fi

# ── 3. Create kind cluster ─────────────────────────────────────
if "$BIN_DIR/kind" get clusters 2>/dev/null | grep -q "ifr-cluster"; then
    info "kind cluster 'ifr-cluster' already exists"
else
    info "Creating kind cluster 'ifr-cluster' (1 CP + 2 workers)..."
    "$BIN_DIR/kind" create cluster --name ifr-cluster --config - <<'KINDEOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
KINDEOF
    info "kind cluster created"
fi

# ── 4. Connect kind to Docker network ──────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -x "$SCRIPT_DIR/connect_kind_to_docker.sh" ]; then
    info "Connecting kind cluster to docker-compose network..."
    bash "$SCRIPT_DIR/connect_kind_to_docker.sh" || warn "kind-docker connection failed (non-critical)"
fi

# ── 5. Verify ──────────────────────────────────────────────────
export PATH="$BIN_DIR:$PATH"
echo ""
echo "========================================="
echo "  环境依赖状态"
echo "========================================="
echo "  kubectl : $(kubectl version --client --short 2>/dev/null | head -1 || echo 'ok')"
echo "  kind    : $(kind version 2>/dev/null)"
echo "  K8s     : $(kubectl get nodes -o name 2>/dev/null | wc -l) nodes"
echo "  Redis   : $(redis-cli -a dev-pass --no-auth-warning ping 2>/dev/null || echo 'check docker: redis:6379')"
echo "  MySQL   : $(mysql -u ifr_app -pdev-pass -h 127.0.0.1 -e 'SELECT 1' industrial_db 2>/dev/null | tail -1 || echo 'check docker: mysql:3306')"
echo ""
echo "  Add to your shell: export PATH=\"$BIN_DIR:\$PATH\""
echo "  Docker kubeconfig: ~/.kube/config.docker"
echo "========================================="
