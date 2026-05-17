#!/usr/bin/env bash

set -euo pipefail

echo "=== K3s Agent Auto Join ==="

# =========================
# HARD-CODED CONFIG
# =========================

K3S_MASTER_IP="100.76.120.58"

K8S_MASTER="https://${K3S_MASTER_IP}:6443"

MINIO_ENDPOINT="http://minio-api.minio.svc.cluster.local:9000"

REGISTRY="100.76.120.58:5000"

TOKEN="K10c3e48380ca4f99c6eb5dee3245142561a4c6cae137db501574b6b6eb3eaea302::server:08bf29a87a9d71ef872b0e4010ac6ae8"

# =========================
# CHECK TAILSCALE
# =========================

echo "=== Checking Tailscale ==="

if ! command -v tailscale >/dev/null 2>&1; then
  echo "tailscale is not installed"
  echo
  echo "Install with:"
  echo "curl -fsSL https://tailscale.com/install.sh | sh"
  exit 1
fi

# =========================
# GET TAILSCALE IP
# =========================

echo "=== Detecting Tailscale IP ==="

TAILSCALE_IP="$(tailscale ip -4 || true)"

if [ -z "$TAILSCALE_IP" ]; then
  echo "Cannot detect Tailscale IPv4"
  echo
  echo "Run:"
  echo "sudo tailscale up"
  exit 1
fi

echo "Detected node IP: $TAILSCALE_IP"

# =========================
# CREATE K3S CONFIG DIR
# =========================

echo "=== Preparing K3s config ==="

sudo mkdir -p /etc/rancher/k3s

# =========================
# CONFIGURE REGISTRY
# =========================

if [ -n "${REGISTRY}" ]; then
  echo "=== Configuring local registry ==="

  cat <<EOF | sudo tee /etc/rancher/k3s/registries.yaml >/dev/null
mirrors:
  "${REGISTRY}":
    endpoint:
      - "http://${REGISTRY}"

configs:
  "${REGISTRY}":
    tls:
      insecure_skip_verify: true
EOF

fi

# =========================
# INSTALL K3S AGENT
# =========================

echo "=== Installing K3s agent ==="

curl -sfL https://get.k3s.io | \
  K3S_URL="${K8S_MASTER}" \
  K3S_TOKEN="${TOKEN}" \
  INSTALL_K3S_EXEC="agent --node-ip=${TAILSCALE_IP} --flannel-iface=tailscale0" \
  sh -

# =========================
# DONE
# =========================

echo
echo "=== SUCCESS ==="
echo "Node joined cluster successfully"
echo
echo "Check on master:"
echo "k"