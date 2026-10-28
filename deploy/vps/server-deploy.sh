#!/usr/bin/env bash
# One-command deploy run ON the VPS from a checkout of this repo (the
# alternative to `make vps-deploy`, which builds on the operator machine and
# ships the image over ssh):
#
#   git clone https://github.com/resyahrial/sensor-upload-server.git
#   cd sensor-upload-server && sudo deploy/vps/server-deploy.sh
#
# Later deploys: git pull && sudo deploy/vps/server-deploy.sh
#
# What it does (safe to re-run):
#   1. bootstrap.sh on the first run (Docker, DATA_DIR, ufw, QUIC sysctls);
#      skipped once Docker and DATA_DIR/certs exist (BOOTSTRAP=1 forces it)
#   2. gen-secrets.sh --ip PUBLIC_IP into deploy/vps/secrets (existing files
#      are kept; ca.key stays there, so keep the checkout private)
#   3. docker build of the root Dockerfile, tagged vps-<commit>-<time> and :vps
#   4. installs docker-compose.yml, .env and certs into DATA_DIR
#   5. docker compose up -d (restarts the server if certs changed), waits for
#      /readyz and probes https://PUBLIC_IP:PUBLIC_PORT/rest/files/
#
# Environment:
#   PUBLIC_IP     public IP for the TLS certificate (default: detected via
#                 api.ipify.org)
#   PUBLIC_PORT   default 9000; must match HTTPS_PORT in .env if you set it
#   SSH_PORT      default 22 (allowed in ufw on bootstrap)
#   UFW           1 (default) to configure ufw on bootstrap, 0 to skip
#   SAN           extra certificate SANs, e.g. DNS:upload.example.com
#   DATA_DIR      default /srv/sensor-upload
#   BOOTSTRAP     1 to run bootstrap.sh even if the host looks set up
#   READY_TIMEOUT seconds to wait for /readyz (default 180)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)

PUBLIC_IP=${PUBLIC_IP:-}
PUBLIC_PORT=${PUBLIC_PORT:-9000}
SSH_PORT=${SSH_PORT:-22}
UFW=${UFW:-1}
SAN=${SAN:-}
DATA_DIR=${DATA_DIR:-/srv/sensor-upload}
BOOTSTRAP=${BOOTSTRAP:-0}
READY_TIMEOUT=${READY_TIMEOUT:-180}
SECRETS_DIR=$here/secrets
IMAGE_REPO=localhost/sensor-upload-server
SERVER_UID=65532

log() { printf '\033[1mserver-deploy:\033[0m %s\n' "$*"; }
die() { printf 'server-deploy: %s\n' "$*" >&2; exit 1; }

[[ $(id -u) == 0 ]] || die "run as root: sudo $0"

if [[ -z $PUBLIC_IP ]]; then
	PUBLIC_IP=$(curl -fsS --max-time 10 https://api.ipify.org) ||
		die "could not detect the public IP; set PUBLIC_IP=..."
	log "detected public IP $PUBLIC_IP"
fi

if [[ $BOOTSTRAP == 1 ]] || ! command -v docker >/dev/null || [[ ! -d $DATA_DIR/certs ]]; then
	log "bootstrapping the host"
	DATA_DIR="$DATA_DIR" SSH_PORT="$SSH_PORT" PUBLIC_PORT="$PUBLIC_PORT" UFW="$UFW" \
		bash "$here/bootstrap.sh"
fi

log "secrets (existing files are kept)"
"$here/gen-secrets.sh" --ip "$PUBLIC_IP" ${SAN:+--san "$SAN"} --out "$SECRETS_DIR"

tag="vps-$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo nogit)-$(date -u +%Y%m%d%H%M%S)"
log "building $IMAGE_REPO:$tag"
docker build -t "$IMAGE_REPO:$tag" -t "$IMAGE_REPO:vps" "$root"

log "installing compose file, .env and certificates into $DATA_DIR"
install -m 644 "$here/docker-compose.yml" "$DATA_DIR/docker-compose.yml"
install -m 600 "$SECRETS_DIR/.env" "$DATA_DIR/.env"
certs_changed=0
for f in server.crt server.key jwt.key ca.crt; do
	if ! cmp -s "$SECRETS_DIR/$f" "$DATA_DIR/certs/$f"; then
		install -m 640 -g "$SERVER_UID" "$SECRETS_DIR/$f" "$DATA_DIR/certs/$f"
		certs_changed=1
	fi
done

cd "$DATA_DIR"
log "docker compose up -d"
docker compose up -d --remove-orphans
if [[ $certs_changed == 1 ]]; then
	log "certificates or JWT key changed; restarting server"
	docker compose restart server
fi

log "waiting up to ${READY_TIMEOUT}s for /readyz"
metrics_port=$(sed -n 's/^METRICS_PORT=//p' .env)
end=$(($(date +%s) + READY_TIMEOUT))
until body=$(curl -fsS "http://127.0.0.1:${metrics_port:-9100}/readyz" 2>/dev/null); do
	if [[ $(date +%s) -ge $end ]]; then
		docker compose ps
		docker compose logs --tail=50 server
		die "server not ready after ${READY_TIMEOUT}s"
	fi
	sleep 2
done
echo "$body"

url="https://$PUBLIC_IP:$PUBLIC_PORT"
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 --cacert certs/ca.crt "$url/rest/files/" || true)
if [[ $code == 000 ]]; then
	log "WARNING: $url not reachable from this host (provider firewall? hairpin NAT?)"
else
	log "$url/rest/files/ answers HTTP $code (401 without a token is expected)"
fi

# Keep the three newest versioned images for rollback.
docker images --format '{{.Repository}}:{{.Tag}}' "$IMAGE_REPO" | grep ':vps-' | tail -n +4 |
	xargs -r docker rmi >/dev/null 2>&1 || true

log "deployed $IMAGE_REPO:$tag"
log "clients trust $SECRETS_DIR/ca.crt; devices: cd $DATA_DIR && docker compose exec server /devicectl batch-import -device <id>"
