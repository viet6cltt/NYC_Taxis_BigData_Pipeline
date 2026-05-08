# Setup K3s Agent

File này dùng để setup một máy khác làm K3s agent và join vào K3s master hiện tại.

## 1. Chuẩn bị trên master

Copy file `.env` sang máy agent.

## 2. Cài Tailscale trên máy agent

K3s cluster này dùng Tailscale IP cho node networking, nên máy agent cần join cùng tailnet với master trước.

Ubuntu/Debian/Fedora:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
tailscale ip -4
```

Sau khi `sudo tailscale up`, mở link đăng nhập nếu Tailscale yêu cầu. Lệnh `tailscale ip -4` phải trả về IP dạng `100.x.x.x`.

## 3. Chạy setup tự động trên máy agent

Đứng ở thư mục repo trên máy agent, chạy:

```bash
set -a
. ./.env
set +a

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

TAILSCALE_IP="$(tailscale ip -4)"

if [ -z "$TAILSCALE_IP" ]; then
  echo "Cannot detect Tailscale IPv4. Run: sudo tailscale up"
  exit 1
fi

sudo mkdir -p /etc/rancher/k3s

if [ -n "$REGISTRY" ]; then
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
```

Script trên sẽ:

- Load cấu hình từ `.env`.
- Kiểm tra `K3S_MASTER_IP` và `TOKEN`.
- Lấy node IP bằng `tailscale ip -4`.
- Cấu hình insecure local registry tại `/etc/rancher/k3s/registries.yaml` nếu có `REGISTRY`.
- Cài K3s agent với `--node-ip=<TAILSCALE_IP>` và `--flannel-iface=tailscale0`.
- Join vào master qua `https://<K3S_MASTER_IP>:6443`.

## 4. Kiểm tra sau khi setup

Trên máy agent:

```bash
sudo systemctl cat k3s-agent
sudo systemctl status k3s-agent
sudo journalctl -u k3s-agent -f
```

Trên máy master:

```bash
sudo systemctl cat k3s
kubectl get nodes -o wide
```

Nếu agent join thành công, node mới sẽ xuất hiện trong danh sách.

## 5. File location

Trên master:

```bash
~/.kube/config
/etc/systemd/system/k3s.service
```

Trên agent:

```bash
/etc/systemd/system/k3s-agent.service
```

## 6. Cài thêm NFS client nếu workload cần mount NFS

Ubuntu/Debian:

```bash
sudo apt update
sudo apt install -y nfs-common
```

Fedora:

```bash
sudo dnf install -y nfs-utils
```

## 7. Gỡ và cài lại agent khi cần

Nếu setup sai token hoặc sai IP, gỡ agent rồi chạy lại script setup:

```bash
sudo /usr/local/bin/k3s-agent-uninstall.sh
```

Sau đó kiểm tra lại `.env` và chạy lại bước 3.

## 8. Mở tường lửa
### Cách chuẩn
- Fedora:
```bash
sudo firewall-cmd --permanent --zone=trusted --add-interface=flannel.1
sudo firewall-cmd --permanent --zone=trusted --add-interface=cni0
sudo firewall-cmd --permanent --zone=trusted --add-interface=tailscale0


sudo firewall-cmd --reload
```

- Ubuntu:
```bash
sudo ufw allow in on flannel.1
sudo ufw allow in on cni0
sudo ufw allow in on tailscale0

sudo ufw reload
```
### Cách mạnh nhất
- Fedora
```bash
sudo systemctl stop firewalld
```

- Ubuntu: 
```bash
sudo ufw disable
```
