#!/usr/bin/env bash
set -euo pipefail

echo "=== K3s Agent Join for VM ==="

if [ ! -f ./.env ]; then
  echo "Missing .env file"
  exit 1
fi

set -a
# shellcheck disable=SC1091
. ./.env
set +a

K3S_MASTER_IP="${K3S_MASTER_IP:-}"
TOKEN="${TOKEN:-}"
REGISTRY="${REGISTRY:-}"

if [ -z "$K3S_MASTER_IP" ]; then
  echo "Missing K3S_MASTER_IP in .env"
  exit 1
fi

if [ -z "$TOKEN" ]; then
  echo "Missing TOKEN in .env"
  exit 1
fi

if ! command -v tailscale >/dev/null 2>&1; then
  echo "tailscale is not installed. Install and login to Tailscale first."
  exit 1
fi

TAILSCALE_IP="$(tailscale ip -4 2>/dev/null || true)"

if [ -z "$TAILSCALE_IP" ]; then
  echo "Cannot detect Tailscale IPv4. Run: sudo tailscale up"
  exit 1
fi

echo "Master API: https://${K3S_MASTER_IP}:6443"
echo "Agent node IP: ${TAILSCALE_IP}"

sudo mkdir -p /etc/rancher/k3s

if [ -n "$REGISTRY" ]; then
  echo "Configuring registry mirror: ${REGISTRY}"
  cat <<EOF | sudo tee /etc/rancher/k3s/registries.yaml >/dev/null
mirrors:
  "$REGISTRY":
    endpoint:
      - "http://$REGISTRY"
configs:
  "$REGISTRY":
    tls:
      insecure_skip_verify: true
EOF
fi

curl -sfL https://get.k3s.io | \
  K3S_URL="https://${K3S_MASTER_IP}:6443" \
  K3S_TOKEN="$TOKEN" \
  INSTALL_K3S_EXEC="agent --node-ip=${TAILSCALE_IP} --flannel-iface=tailscale0" \
  sh -

echo
echo "=== SUCCESS ==="
echo "VM agent joined the K3s cluster."
echo "Check on master: kubectl get nodes -o wide"
