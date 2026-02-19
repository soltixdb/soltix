#!/usr/bin/env bash
set -euo pipefail

# Install Docker + Docker Compose on AWS EC2
# Usage:
#   sudo bash scripts/install_docker_etcd_ec2.sh

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Please run as root (sudo)."
    exit 1
  fi
}

install_docker_compose_binary() {
  local compose_version os arch target_dir target_file
  compose_version="2.39.2"
  os="linux"
  arch="$(uname -m)"

  case "$arch" in
    x86_64) arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *)
      echo "Unsupported architecture for docker compose: $arch"
      exit 1
      ;;
  esac

  target_dir="/usr/local/lib/docker/cli-plugins"
  target_file="${target_dir}/docker-compose"
  mkdir -p "$target_dir"

  log "Installing Docker Compose v${compose_version} binary..."
  curl -fL "https://github.com/docker/compose/releases/download/v${compose_version}/docker-compose-${os}-${arch}" -o "$target_file"
  chmod +x "$target_file"

  ln -sf "$target_file" /usr/local/bin/docker-compose
}

ensure_compose() {
  if docker compose version >/dev/null 2>&1; then
    log "Docker Compose plugin is available"
    return 0
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    log "Standalone docker-compose is available"
    return 0
  fi

  install_docker_compose_binary

  if docker compose version >/dev/null 2>&1 || command -v docker-compose >/dev/null 2>&1; then
    log "Docker Compose installed successfully"
  else
    echo "Docker Compose installation failed."
    exit 1
  fi
}

install_docker() {
  if command -v docker >/dev/null 2>&1; then
    log "Docker already installed: $(docker --version)"
  else
    if command -v apt-get >/dev/null 2>&1; then
      log "Installing Docker via apt-get..."
      apt-get update -y
      apt-get install -y ca-certificates curl gnupg lsb-release
      install -m 0755 -d /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      chmod a+r /etc/apt/keyrings/docker.gpg
      echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" >/etc/apt/sources.list.d/docker.list
      apt-get update -y
      apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    elif command -v dnf >/dev/null 2>&1; then
      log "Installing Docker via dnf..."
      dnf -y install docker
    elif command -v yum >/dev/null 2>&1; then
      log "Installing Docker via yum..."
      yum -y install docker
    else
      echo "Unsupported package manager. Please install Docker manually."
      exit 1
    fi
  fi

  log "Enabling Docker auto-start and starting service..."
  systemctl enable docker
  systemctl restart docker

  ensure_compose

  for user_name in ec2-user ubuntu; do
    if id "$user_name" >/dev/null 2>&1; then
      usermod -aG docker "$user_name" || true
    fi
  done

  log "Docker installation completed."
  log "Docker service status: $(systemctl is-enabled docker) / $(systemctl is-active docker)"
  log "Docker version: $(docker --version)"

  if docker compose version >/dev/null 2>&1; then
    log "Docker Compose version: $(docker compose version --short 2>/dev/null || docker compose version | head -n1)"
  elif command -v docker-compose >/dev/null 2>&1; then
    log "Docker Compose version: $(docker-compose version --short 2>/dev/null || docker-compose version | head -n1)"
  fi

  log "If current SSH user was added to docker group, run: newgrp docker (or relogin)."
}

main() {
  require_root
  install_docker
}

main "$@"
