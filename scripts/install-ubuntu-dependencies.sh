#!/bin/sh
set -eu

usage() {
  cat <<'EOF'
Usage: scripts/install-ubuntu-dependencies.sh [options]

Installs the Linux packages needed to run the production Docker Compose stack on
an Ubuntu VM, including Docker Engine, the Docker Compose plugin, Quarto, git,
and small operator utilities.

Options:
  --hostname NAME        Set the VM hostname. Defaults to realgooddata.
  --configure-firewall   Enable UFW and allow SSH, HTTP, and HTTPS.
  --skip-docker-group    Do not add a non-root user to the docker group.
  --skip-hostname        Do not change the VM hostname.
  --skip-quarto          Do not install Quarto.
  --user USER            Add USER to the docker group instead of the sudo user.
  -h, --help             Show this help text.

Examples:
  sudo ./scripts/install-ubuntu-dependencies.sh
  sudo ./scripts/install-ubuntu-dependencies.sh --configure-firewall
EOF
}

CONFIGURE_FIREWALL=0
SKIP_DOCKER_GROUP=0
SKIP_HOSTNAME=0
SKIP_QUARTO=0
TARGET_HOSTNAME="realgooddata"
TARGET_USER="${SUDO_USER:-}"

detect_target_user() {
  if [ -n "${TARGET_USER}" ]; then
    return
  fi

  if [ "$(id -u)" -ne 0 ]; then
    TARGET_USER="$(id -un)"
    return
  fi

  if command -v logname >/dev/null 2>&1; then
    DETECTED_USER="$(logname 2>/dev/null || true)"
    if [ -n "${DETECTED_USER}" ] && [ "${DETECTED_USER}" != "root" ]; then
      TARGET_USER="${DETECTED_USER}"
      return
    fi
  fi

  if command -v who >/dev/null 2>&1; then
    DETECTED_USER="$(who am i 2>/dev/null | awk '{print $1}' || true)"
    if [ -n "${DETECTED_USER}" ] && [ "${DETECTED_USER}" != "root" ]; then
      TARGET_USER="${DETECTED_USER}"
    fi
  fi
}

detect_target_user

while [ "$#" -gt 0 ]; do
  case "$1" in
    --hostname)
      shift
      if [ "$#" -eq 0 ]; then
        echo "Missing value for --hostname" >&2
        exit 1
      fi
      TARGET_HOSTNAME="$1"
      ;;
    --configure-firewall)
      CONFIGURE_FIREWALL=1
      ;;
    --skip-docker-group)
      SKIP_DOCKER_GROUP=1
      ;;
    --skip-hostname)
      SKIP_HOSTNAME=1
      ;;
    --skip-quarto)
      SKIP_QUARTO=1
      ;;
    --user)
      shift
      if [ "$#" -eq 0 ]; then
        echo "Missing value for --user" >&2
        exit 1
      fi
      TARGET_USER="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [ "$(id -u)" -eq 0 ]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "Run this script as root, or install sudo first." >&2
  exit 1
fi

run() {
  if [ -n "${SUDO}" ]; then
    "${SUDO}" "$@"
  else
    "$@"
  fi
}

configure_hostname() {
  if [ "${SKIP_HOSTNAME}" -eq 1 ]; then
    echo "Skipping hostname configuration."
    return
  fi

  case "${TARGET_HOSTNAME}" in
    *[!A-Za-z0-9-]*|""|-*|*-)
      echo "Invalid hostname: ${TARGET_HOSTNAME}" >&2
      exit 1
      ;;
  esac

  echo "Setting hostname to ${TARGET_HOSTNAME}..."
  if command -v hostnamectl >/dev/null 2>&1; then
    run hostnamectl set-hostname "${TARGET_HOSTNAME}"
  else
    HOSTNAME_FILE="$(mktemp)"
    printf '%s\n' "${TARGET_HOSTNAME}" > "${HOSTNAME_FILE}"
    run install -m 0644 "${HOSTNAME_FILE}" /etc/hostname
    rm -f "${HOSTNAME_FILE}"
    run hostname "${TARGET_HOSTNAME}"
  fi

  HOSTS_FILE="$(mktemp)"
  awk -v hostname="${TARGET_HOSTNAME}" '
    BEGIN { updated = 0 }
    /^127[.]0[.]1[.]1[[:space:]]/ && updated == 0 {
      print "127.0.1.1\t" hostname
      updated = 1
      next
    }
    { print }
    END {
      if (updated == 0) {
        print "127.0.1.1\t" hostname
      }
    }
  ' /etc/hosts > "${HOSTS_FILE}"
  run install -m 0644 "${HOSTS_FILE}" /etc/hosts
  rm -f "${HOSTS_FILE}"
}

install_quarto() {
  if [ "${SKIP_QUARTO}" -eq 1 ]; then
    echo "Skipping Quarto installation."
    return
  fi

  case "$(dpkg --print-architecture)" in
    amd64)
      QUARTO_ASSET_SUFFIX="linux-amd64.deb"
      ;;
    arm64)
      QUARTO_ASSET_SUFFIX="linux-arm64.deb"
      ;;
    *)
      echo "Quarto installer is not configured for architecture: $(dpkg --print-architecture)" >&2
      exit 1
      ;;
  esac

  echo "Resolving latest Quarto release..."
  QUARTO_TMP_DIR="$(mktemp -d)"
  QUARTO_METADATA="${QUARTO_TMP_DIR}/download.json"
  curl -fsSL https://quarto.org/docs/download/_download.json -o "${QUARTO_METADATA}"

  QUARTO_VERSION="$(jq -r '.version' "${QUARTO_METADATA}")"
  QUARTO_ASSET_NAME="$(jq -r --arg suffix "${QUARTO_ASSET_SUFFIX}" '.assets[] | select(.name | endswith($suffix)) | .name' "${QUARTO_METADATA}")"
  QUARTO_DOWNLOAD_URL="$(jq -r --arg name "${QUARTO_ASSET_NAME}" '.assets[] | select(.name == $name) | .download_url' "${QUARTO_METADATA}")"
  QUARTO_CHECKSUM="$(jq -r --arg name "${QUARTO_ASSET_NAME}" '.assets[] | select(.name == $name) | .checksum' "${QUARTO_METADATA}")"

  if [ -z "${QUARTO_VERSION}" ] || [ "${QUARTO_VERSION}" = "null" ] \
    || [ -z "${QUARTO_ASSET_NAME}" ] || [ "${QUARTO_ASSET_NAME}" = "null" ] \
    || [ -z "${QUARTO_DOWNLOAD_URL}" ] || [ "${QUARTO_DOWNLOAD_URL}" = "null" ] \
    || [ -z "${QUARTO_CHECKSUM}" ] || [ "${QUARTO_CHECKSUM}" = "null" ]; then
    rm -rf "${QUARTO_TMP_DIR}"
    echo "Could not resolve a Quarto .deb download from Quarto metadata." >&2
    exit 1
  fi

  if command -v quarto >/dev/null 2>&1; then
    INSTALLED_QUARTO_VERSION="$(quarto --version 2>/dev/null || true)"
    if [ "${INSTALLED_QUARTO_VERSION}" = "${QUARTO_VERSION}" ]; then
      echo "Quarto ${QUARTO_VERSION} is already installed."
      rm -rf "${QUARTO_TMP_DIR}"
      return
    fi
  fi

  QUARTO_DEB="${QUARTO_TMP_DIR}/${QUARTO_ASSET_NAME}"
  echo "Downloading Quarto ${QUARTO_VERSION}..."
  curl -fsSL "${QUARTO_DOWNLOAD_URL}" -o "${QUARTO_DEB}"

  echo "Verifying Quarto checksum..."
  printf '%s  %s\n' "${QUARTO_CHECKSUM}" "${QUARTO_DEB}" | sha256sum -c -

  echo "Installing Quarto ${QUARTO_VERSION}..."
  run apt-get install -y "${QUARTO_DEB}"
  rm -rf "${QUARTO_TMP_DIR}"
}

if [ ! -r /etc/os-release ]; then
  echo "Cannot detect the operating system; /etc/os-release is missing." >&2
  exit 1
fi

. /etc/os-release

if [ "${ID:-}" != "ubuntu" ]; then
  echo "This script is intended for Ubuntu. Detected: ${PRETTY_NAME:-unknown OS}" >&2
  exit 1
fi

DOCKER_CODENAME="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
if [ -z "${DOCKER_CODENAME}" ]; then
  echo "Cannot detect the Ubuntu codename for Docker's apt repository." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

configure_hostname

echo "Installing base packages..."
run apt-get update
run apt-get install -y \
  ca-certificates \
  curl \
  git \
  gnupg \
  jq \
  lsb-release \
  ufw \
  unattended-upgrades \
  unzip

install_quarto

CONFLICTING_PACKAGES=""
for package in docker.io docker-compose docker-compose-v2 docker-doc podman-docker containerd runc; do
  if dpkg -s "${package}" >/dev/null 2>&1; then
    CONFLICTING_PACKAGES="${CONFLICTING_PACKAGES} ${package}"
  fi
done

if [ -n "${CONFLICTING_PACKAGES}" ]; then
  echo "Removing conflicting Docker packages:${CONFLICTING_PACKAGES}"
  # shellcheck disable=SC2086
  run apt-get remove -y ${CONFLICTING_PACKAGES}
fi

echo "Configuring Docker's official apt repository..."
run install -m 0755 -d /etc/apt/keyrings
run curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
run chmod a+r /etc/apt/keyrings/docker.asc

ARCHITECTURE="$(dpkg --print-architecture)"
DOCKER_SOURCE="$(mktemp)"
printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu %s stable\n' \
  "${ARCHITECTURE}" \
  "${DOCKER_CODENAME}" > "${DOCKER_SOURCE}"
run install -m 0644 "${DOCKER_SOURCE}" /etc/apt/sources.list.d/docker.list
rm -f "${DOCKER_SOURCE}"

echo "Installing Docker Engine and Docker Compose plugin..."
run apt-get update
run apt-get install -y \
  containerd.io \
  docker-buildx-plugin \
  docker-ce \
  docker-ce-cli \
  docker-compose-plugin

DOCKER_RESTART_REQUIRED=0
run install -m 0755 -d /etc/docker
if [ ! -f /etc/docker/daemon.json ]; then
  DOCKER_DAEMON_CONFIG="$(mktemp)"
  cat > "${DOCKER_DAEMON_CONFIG}" <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF
  run install -m 0644 "${DOCKER_DAEMON_CONFIG}" /etc/docker/daemon.json
  rm -f "${DOCKER_DAEMON_CONFIG}"
  DOCKER_RESTART_REQUIRED=1
else
  echo "Leaving existing /etc/docker/daemon.json in place."
fi

if command -v systemctl >/dev/null 2>&1; then
  echo "Enabling Docker services..."
  run systemctl enable --now containerd.service
  run systemctl enable --now docker.service
  if [ "${DOCKER_RESTART_REQUIRED}" -eq 1 ]; then
    run systemctl restart docker.service
  fi
fi

if [ "${SKIP_DOCKER_GROUP}" -eq 0 ] && [ -n "${TARGET_USER}" ] && [ "${TARGET_USER}" != "root" ]; then
  if id "${TARGET_USER}" >/dev/null 2>&1; then
    echo "Adding ${TARGET_USER} to the docker group..."
    if ! getent group docker >/dev/null 2>&1; then
      run groupadd docker
    fi
    run usermod -aG docker "${TARGET_USER}"
    echo "Log out and back in before running docker without sudo."
  else
    echo "Skipping docker group membership; user does not exist: ${TARGET_USER}" >&2
  fi
fi

if [ "${CONFIGURE_FIREWALL}" -eq 1 ]; then
  echo "Configuring UFW for SSH, HTTP, and HTTPS..."
  run ufw allow 22/tcp
  run ufw allow 80/tcp
  run ufw allow 443/tcp
  run ufw --force enable
else
  echo "UFW is installed but not enabled. Re-run with --configure-firewall to enable it."
fi

echo "Verifying Docker installation..."
run docker info >/dev/null
docker --version
docker compose version
if command -v quarto >/dev/null 2>&1; then
  quarto --version
else
  echo "Quarto is not installed."
fi

echo "Ubuntu dependency installation complete."
