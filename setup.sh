#!/usr/bin/env bash
# ============================================================
# Oracle Cloud ARM - AIOStreams + *ARR + nzbDAV Setup Script
# Run as: bash setup.sh
# ============================================================
set -e

echo "=== [1/5] Updating system packages ==="
sudo apt-get update && sudo apt-get upgrade -y

echo "=== [2/5] Installing Docker CE (ARM64) ==="
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER

echo "=== [3/5] Opening firewall ports (Oracle iptables) ==="
sudo iptables -I INPUT -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 81 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 443 -j ACCEPT
sudo apt-get install -y iptables-persistent
sudo netfilter-persistent save

echo "=== [4/5] Generating secret keys ==="
AIO_SECRET=$(openssl rand -hex 32)
US_SECRET=$(openssl rand -hex 16)
sed -i "s/REPLACE_WITH_64_CHAR_HEX_KEY/$AIO_SECRET/" .env.aiostreams
sed -i "s/REPLACE_WITH_SECRET/$US_SECRET/" .env.usenetstreamer
echo "AIOStreams SECRET_KEY: $AIO_SECRET"
echo "UsenetStreamer SECRET:  $US_SECRET"
echo "(Save these somewhere safe!)"

echo "=== [5/5] Pulling Docker images ==="
newgrp docker <<EONG
docker compose pull
EONG

echo ""
echo "=============================================="
echo "Setup complete! Next steps:"
echo "1. Edit .env.arr  — set PUID/PGID/TZ"
echo "2. Run: docker compose up -d"
echo "3. Open http://YOUR_VM_IP:81 for NPM"
echo "4. Add proxy hosts per NPM_PROXY_REFERENCE.txt"
echo "5. Configure nzbDAV with your Usenet provider"
echo "6. Add nzbDAV as SABnzbd client in Radarr/Sonarr"
echo "7. Configure UsenetStreamer via its web UI"
echo "8. Add UsenetStreamer manifest URL to AIOStreams"
echo "=============================================="
