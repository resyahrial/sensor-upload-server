#!/usr/bin/env bash
# One-time VPS setup, run as root on the VPS (Ubuntu 24.04 LTS; Debian 12 works
# too). `make vps-bootstrap VPS=root@<ip>` pipes it over ssh:
#
#   ssh root@<ip> 'bash -s' < deploy/vps/bootstrap.sh
#
# What it does (safe to re-run):
#   1. installs Docker Engine + compose plugin from Docker's apt repository,
#      plus curl, rsync and ufw
#   2. creates DATA_DIR (/srv/sensor-upload) with pgdata/ minio/ staging/ certs/
#      owned by the users the containers run as
#   3. ufw: allow SSH_PORT/tcp, 443/tcp, 443/udp, 9090/tcp; deny other inbound; enable
#   4. raises the UDP socket buffer limits for QUIC (quic-go recommends
#      7500000 bytes), persisted in /etc/sysctl.d
#
# Environment:
#   DEPLOY_USER   non-root user that will run `make vps-deploy` (added to the
#                 docker group, owns DATA_DIR); default root
#   DATA_DIR      default /srv/sensor-upload
#   SSH_PORT      default 22
#   QUIC_SYSCTL   1 (default) to set net.core.rmem_max/wmem_max, 0 to skip
#   UFW           1 (default) to configure and enable ufw, 0 to skip
set -euo pipefail

DEPLOY_USER=${DEPLOY_USER:-root}
DATA_DIR=${DATA_DIR:-/srv/sensor-upload}
SSH_PORT=${SSH_PORT:-22}
QUIC_SYSCTL=${QUIC_SYSCTL:-1}
UFW=${UFW:-1}

# Container users: postgres:16-alpine runs as uid/gid 70, the distroless
# server as 65532 (nonroot), MinIO as root.
PG_UID=70
SERVER_UID=65532

log() { printf 'bootstrap: %s\n' "$*"; }

if [[ $(id -u) != 0 ]]; then
	echo "bootstrap: run as root (ssh root@<ip> or sudo)" >&2
	exit 1
fi
# shellcheck source=/dev/null
. /etc/os-release
case ${ID:-} in
ubuntu | debian) ;;
*) echo "bootstrap: unsupported distribution '${ID:-unknown}' (need Ubuntu or Debian)" >&2; exit 1 ;;
esac
if ! id "$DEPLOY_USER" >/dev/null 2>&1; then
	echo "bootstrap: DEPLOY_USER $DEPLOY_USER does not exist" >&2
	exit 1
fi

export DEBIAN_FRONTEND=noninteractive

log "installing base packages"
apt-get update -q
apt-get install -y -q ca-certificates curl rsync ufw

if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
	log "installing Docker Engine + compose plugin from download.docker.com ($ID $VERSION_CODENAME)"
	# Distribution packages that conflict with docker-ce (Docker's install docs).
	for p in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
		apt-get remove -y -q "$p" >/dev/null 2>&1 || true
	done
	install -m 0755 -d /etc/apt/keyrings
	curl -fsSL "https://download.docker.com/linux/$ID/gpg" -o /etc/apt/keyrings/docker.asc
	chmod a+r /etc/apt/keyrings/docker.asc
	cat >/etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/$ID
Suites: $VERSION_CODENAME
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
	apt-get update -q
	apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
else
	log "docker $(docker --version | awk '{print $3}' | tr -d ,) and compose plugin already installed"
fi
systemctl enable --now docker
docker compose version

if [[ $DEPLOY_USER != root ]]; then
	# Membership in the docker group is root-equivalent on this host.
	usermod -aG docker "$DEPLOY_USER"
	log "added $DEPLOY_USER to the docker group (log in again for it to apply)"
fi

log "creating $DATA_DIR"
deploy_group=$(id -gn "$DEPLOY_USER")
install -d -m 0755 -o "$DEPLOY_USER" -g "$deploy_group" "$DATA_DIR"
install -d -m 0700 -o "$PG_UID" -g "$PG_UID" "$DATA_DIR/pgdata"
# 0755: the server mounts it read-only as DISK_PATH (statfs for the watermarks).
install -d -m 0755 -o root -g root "$DATA_DIR/minio"
install -d -m 0750 -o "$SERVER_UID" -g "$SERVER_UID" "$DATA_DIR/staging"
# certs/: written by DEPLOY_USER, readable by the server container's group
# only. setgid so copied files inherit group 65532 (deploy.sh uses mode 640).
install -d -m 2750 -o "$DEPLOY_USER" -g "$SERVER_UID" "$DATA_DIR/certs"

if [[ $QUIC_SYSCTL == 1 ]]; then
	log "raising UDP buffer limits for QUIC"
	cat >/etc/sysctl.d/60-sensor-upload-quic.conf <<'EOF'
# quic-go wants ~7.5 MB UDP socket buffers
# (https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes).
net.core.rmem_max=7500000
net.core.wmem_max=7500000
EOF
	sysctl -q -p /etc/sysctl.d/60-sensor-upload-quic.conf
fi

if [[ $UFW == 1 ]]; then
	log "configuring ufw (ssh $SSH_PORT/tcp, 443/tcp, 443/udp, 9090/tcp)"
	ufw default deny incoming >/dev/null
	ufw default allow outgoing >/dev/null
	ufw allow "$SSH_PORT/tcp" comment ssh >/dev/null
	ufw allow 443/tcp comment 'sensor-upload https' >/dev/null
	ufw allow 443/udp comment 'sensor-upload http3' >/dev/null
	ufw allow 9090/tcp comment 'sensor-upload grpc' >/dev/null
	ufw --force enable >/dev/null
	ufw status verbose
	# Note: Docker writes its own iptables rules for published ports, which
	# bypass ufw. docker-compose.yml therefore publishes only 443 and 9090
	# publicly and binds everything else to 127.0.0.1.
fi

log "done. Next, on the operator machine: make vps-deploy VPS=$DEPLOY_USER@<ip>"
