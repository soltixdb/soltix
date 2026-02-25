#!/usr/bin/env bash
set -euo pipefail

# Setup systemd service for Soltix Router/Storage on EC2
# Usage:
#   sudo bash setup_systemd_service.sh router /root/router/router-linux-amd64 /root/router/config.yml
#   sudo bash setup_systemd_service.sh storage /root/storage/storage-linux-amd64 /root/storage/config.yml

SERVICE_NAME="${1:-router}"  # router or storage
BINARY_PATH="${2:-/root/${SERVICE_NAME}/${SERVICE_NAME}-linux-amd64}"
CONFIG_PATH="${3:-/root/${SERVICE_NAME}/config.yml}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Please run as root (sudo)."
    exit 1
  fi
}

setup_service() {
  local service_file="/etc/systemd/system/soltix-${SERVICE_NAME}.service"
  local working_dir="$(dirname "$BINARY_PATH")"
  
  if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Binary not found at $BINARY_PATH"
    exit 1
  fi
  
  if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "Error: Config not found at $CONFIG_PATH"
    exit 1
  fi
  
  log "Creating systemd service file: $service_file"
  
  cat > "$service_file" <<EOF
[Unit]
Description=Soltix ${SERVICE_NAME^} Service
Documentation=https://docs.soltixdb.com
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=${working_dir}
ExecStart=${BINARY_PATH} --config ${CONFIG_PATH}
Restart=always
RestartSec=5s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=soltix-${SERVICE_NAME}

# Security settings
NoNewPrivileges=true
PrivateTmp=true

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
EOF

  chmod 644 "$service_file"
  
  log "Reloading systemd daemon..."
  systemctl daemon-reload
  
  log "Enabling service to start on boot..."
  systemctl enable "soltix-${SERVICE_NAME}.service"
  
  log "Starting service..."
  systemctl start "soltix-${SERVICE_NAME}.service"
  
  sleep 2
  
  log "Service status:"
  systemctl status "soltix-${SERVICE_NAME}.service" --no-pager || true
  
  echo ""
  log "Service setup complete!"
  echo ""
  echo "Useful commands:"
  echo "  systemctl status soltix-${SERVICE_NAME}   # Check status"
  echo "  systemctl restart soltix-${SERVICE_NAME}  # Restart service"
  echo "  systemctl stop soltix-${SERVICE_NAME}     # Stop service"
  echo "  systemctl start soltix-${SERVICE_NAME}    # Start service"
  echo "  journalctl -u soltix-${SERVICE_NAME} -f   # View logs"
  echo "  journalctl -u soltix-${SERVICE_NAME} -n 100 --no-pager  # Last 100 logs"
}

main() {
  require_root
  
  if [[ "$SERVICE_NAME" != "router" && "$SERVICE_NAME" != "storage" ]]; then
    echo "Error: SERVICE_NAME must be 'router' or 'storage'"
    exit 1
  fi
  
  setup_service
}

main "$@"
