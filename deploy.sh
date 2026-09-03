#!/usr/bin/env bash
# One-shot VPS setup: installs Python deps and a systemd service that runs the
# bot 24/7 (auto-restarts on crash and on reboot). Run from the repo folder.
set -e
cd "$(dirname "$0")"
DIR="$(pwd)"

if [ ! -f .env ]; then
  echo "No .env found. Run:  cp .env.example .env  then edit it (set ACCESS_TOKEN)."
  exit 1
fi

echo "[1/3] Installing Python + venv…"
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip
python3 -m venv venv
./venv/bin/pip install --upgrade pip >/dev/null
./venv/bin/pip install aiohttp

echo "[2/3] Installing systemd service…"
sudo tee /etc/systemd/system/pumpbot.service >/dev/null <<UNIT
[Unit]
Description=pump paper desk
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=$DIR
EnvironmentFile=$DIR/.env
ExecStart=$DIR/venv/bin/python app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

echo "[3/3] Starting it…"
sudo systemctl daemon-reload
sudo systemctl enable pumpbot
sudo systemctl restart pumpbot
echo ""
echo "Done. It will run forever, even after reboot."
echo "Watch logs:  sudo journalctl -u pumpbot -f"
echo "Dashboard:   http://YOUR_SERVER_IP:8080/?k=YOUR_TOKEN"
