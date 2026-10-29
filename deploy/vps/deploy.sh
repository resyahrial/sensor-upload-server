#!/usr/bin/env bash
# Operator-side VPS control (wrapped by mk/vps.mk). Needs ssh access to the
# VPS (bootstrap.sh run once), Go, docker or podman, rsync and curl locally.
#
#   deploy.sh deploy   cross-compile cmd/server + cmd/devicectl for linux/$VPS_ARCH, build the
#                      runtime image (Dockerfile.prebuilt), docker save | ssh
#                      docker load, rsync compose + certs + .env to REMOTE_DIR,
#                      docker compose up -d, wait for /readyz
#   deploy.sh status   docker compose ps, /readyz, disk usage
#   deploy.sh logs     docker compose logs (SERVICE, TAIL, FOLLOW=1)
#   deploy.sh down     docker compose down (data is kept)
#   deploy.sh wipe     down + delete everything in pgdata/ minio/ staging/ (asks first)
#   deploy.sh device   DEVICE_CMD=approve|reject|revoke|issue|access-token|list
#                      [DEVICE=id] [TTL=24h] [STATUS=active]: run /devicectl in
#                      the server container; issue prints a token pair JSON,
#                      access-token an access JWT (stdout)
#
# Environment:
#   VPS           ssh destination, e.g. root@203.0.113.10 (required)
#   VPS_IP        public IP (default: the host part of VPS; set it when VPS
#                 is an ssh alias)
#   VPS_ARCH      amd64 (default) or arm64
#   SECRETS_DIR   output of gen-secrets.sh (default deploy/vps/secrets)
#   REMOTE_DIR    default /srv/sensor-upload (must match DATA_DIR in .env)
#   SSH_OPTS      extra ssh options, e.g. "-p 2222 -i ~/.ssh/vps"
#   CONTAINER_RT  docker or podman (default: whichever is on PATH)
#   READY_TIMEOUT seconds to wait for /readyz after deploy (default 180)
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)

VPS=${VPS:-}
VPS_ARCH=${VPS_ARCH:-amd64}
SECRETS_DIR=${SECRETS_DIR:-$here/secrets}
REMOTE_DIR=${REMOTE_DIR:-/srv/sensor-upload}
READY_TIMEOUT=${READY_TIMEOUT:-180}
GO=${GO:-go}
IMAGE_REPO=localhost/sensor-upload-server
SERVER_UID=65532

read -ra ssh_opts <<<"${SSH_OPTS:-}"

log() { printf '\033[1mvps:\033[0m %s\n' "$*"; }
die() { printf 'vps: %s\n' "$*" >&2; exit 1; }

[[ -n $VPS ]] || die "VPS is not set (e.g. VPS=root@203.0.113.10)"
vps_host=${VPS_IP:-${VPS#*@}}

# vssh ARGS: ssh to the VPS with SSH_OPTS (empty-array safe for bash 3.2).
# Remote commands are built locally on purpose (SC2029).
# shellcheck disable=SC2029
vssh() { ssh ${ssh_opts[@]+"${ssh_opts[@]}"} "$VPS" "$@"; }
# remote CMD: run CMD (a shell string, expanded locally first) on the VPS in REMOTE_DIR.
remote() { vssh "cd '$REMOTE_DIR' && $1"; }

container_rt() {
	if [[ -n ${CONTAINER_RT:-} ]]; then
		echo "$CONTAINER_RT"
	elif command -v docker >/dev/null; then
		echo docker
	elif command -v podman >/dev/null; then
		echo podman
	else
		die "neither docker nor podman found (set CONTAINER_RT)"
	fi
}

check_secrets() {
	local f
	for f in .env ca.crt server.crt server.key jwt.key; do
		[[ -s $SECRETS_DIR/$f ]] || die "$SECRETS_DIR/$f missing: run make vps-secrets VPS_IP=$vps_host"
	done
	if ! openssl x509 -in "$SECRETS_DIR/server.crt" -noout -ext subjectAltName 2>/dev/null |
		tr -d ' ' | tr ',' '\n' | grep -qxF -e "IPAddress:$vps_host" -e "DNS:$vps_host"; then
		log "WARNING: $vps_host is not in the server certificate's SAN (gen-secrets.sh --renew-cert)"
	fi
}

check_remote() {
	remote "test -d certs && test -d staging && test -d pgdata && test -d minio" ||
		die "$REMOTE_DIR is not prepared on $VPS: run make vps-bootstrap first"
	local gid
	gid=$(remote "stat -c %g certs")
	[[ $gid == "$SERVER_UID" ]] ||
		die "$REMOTE_DIR/certs has group $gid, want $SERVER_UID (re-run make vps-bootstrap)"
	remote "docker compose version >/dev/null" || die "docker compose not usable by $VPS (docker group?)"
}

build_image() {
	local rt tag ctx
	rt=$(container_rt)
	tag="vps-$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo nogit)"
	if [[ -n $(git -C "$root" status --porcelain 2>/dev/null) ]]; then
		tag+="-dirty"
	fi
	tag+="-$(date -u +%Y%m%d%H%M%S)"
	image="$IMAGE_REPO:$tag"
	ctx=$(mktemp -d)
	# shellcheck disable=SC2064
	trap "rm -rf '$ctx'" EXIT

	log "cross-compiling cmd/server and cmd/devicectl for linux/$VPS_ARCH"
	(cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH="$VPS_ARCH" \
		"$GO" build -trimpath -ldflags="-s -w" -o "$ctx/" ./cmd/server ./cmd/devicectl)
	mkdir -p "$ctx/data/staging" "$ctx/data/objects"

	log "building $image ($rt, linux/$VPS_ARCH)"
	"$rt" build --platform "linux/$VPS_ARCH" -f "$here/Dockerfile.prebuilt" -t "$image" "$ctx"

	log "copying the image to $VPS (docker save | ssh docker load)"
	"$rt" save "$image" | gzip -1 | vssh 'gunzip | docker load'
	vssh "docker tag '$image' '$IMAGE_REPO:vps'"
}

sync_files() {
	log "syncing compose file, .env and certificates to $VPS:$REMOTE_DIR"
	local rsh="ssh ${SSH_OPTS:-}"
	rsync -t --chmod=F644 -e "$rsh" "$here/docker-compose.yml" "$VPS:$REMOTE_DIR/docker-compose.yml"
	rsync -t --chmod=F600 -e "$rsh" "$SECRETS_DIR/.env" "$VPS:$REMOTE_DIR/.env"
	# No -p/-g: new files get group 65532 from the setgid certs/ directory,
	# mode 640, so only the server container (and DEPLOY_USER) can read the
	# TLS and JWT signing keys. ca.key stays on this machine.
	certs_changed=$(rsync -ti --checksum --chmod=F640 -e "$rsh" \
		"$SECRETS_DIR/server.crt" "$SECRETS_DIR/server.key" "$SECRETS_DIR/jwt.key" "$SECRETS_DIR/ca.crt" \
		"$VPS:$REMOTE_DIR/certs/")
}

wait_ready() {
	log "waiting up to ${READY_TIMEOUT}s for /readyz"
	# shellcheck disable=SC2016 # expanded on the VPS
	if ! remote 'port=$(sed -n "s/^METRICS_PORT=//p" .env); port=${port:-9100}
		end=$(( $(date +%s) + '"$READY_TIMEOUT"' ))
		until body=$(curl -fsS "http://127.0.0.1:$port/readyz" 2>/dev/null); do
			[ "$(date +%s)" -lt "$end" ] || exit 1
			sleep 2
		done
		echo "$body"'; then
		remote "docker compose ps; docker compose logs --tail=50 server" || true
		die "server not ready after ${READY_TIMEOUT}s"
	fi
	local code port url
	port=$(sed -n 's/^HTTPS_PORT=//p' "$SECRETS_DIR/.env")
	url="https://$vps_host:${port:-9000}"
	code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 --cacert "$SECRETS_DIR/ca.crt" \
		"$url/rest/files/" || true)
	if [[ $code == 000 ]]; then
		log "WARNING: $url/ not reachable from here with ca.crt (firewall? SAN?)"
	else
		log "$url/rest/files/ answers HTTP $code through TLS (401 without a token is expected)"
	fi
}

cmd_deploy() {
	check_secrets
	check_remote
	build_image
	sync_files
	log "docker compose up -d"
	remote "docker compose up -d --remove-orphans"
	if [[ -n $certs_changed ]]; then
		log "certificates or JWT key changed; restarting server"
		remote "docker compose restart server"
	fi
	wait_ready
	# Keep the three newest versioned images for rollback.
	remote "docker images --format '{{.Repository}}:{{.Tag}}' '$IMAGE_REPO' | grep ':vps-' | tail -n +4 | xargs -r docker rmi >/dev/null 2>&1 || true"
	log "deployed $image to $VPS"
}

cmd_status() {
	# shellcheck disable=SC2016 # expanded on the VPS
	remote 'docker compose ps
		port=$(sed -n "s/^METRICS_PORT=//p" .env)
		echo; echo "readyz: $(curl -sS "http://127.0.0.1:${port:-9100}/readyz" 2>&1)"
		echo; df -h .
		echo; du -sh pgdata minio staging 2>/dev/null || sudo -n du -sh pgdata minio staging 2>/dev/null || true
		echo; docker images "'"$IMAGE_REPO"'" --format "{{.Repository}}:{{.Tag}}  {{.CreatedSince}}  {{.Size}}"'
}

cmd_logs() {
	local args="--tail=${TAIL:-200}"
	[[ ${FOLLOW:-0} == 1 ]] && args+=" -f"
	remote "docker compose logs $args ${SERVICE:-}"
}

cmd_down() {
	log "docker compose down on $VPS (data in $REMOTE_DIR is kept)"
	remote "docker compose down --remove-orphans"
}

cmd_wipe() {
	echo "This stops the stack on $VPS and DELETES all uploads, the database and"
	echo "staging data under $REMOTE_DIR/{pgdata,minio,staging}. Certificates and .env are kept."
	local answer
	read -r -p "Type the VPS host ($vps_host) to confirm: " answer
	[[ $answer == "$vps_host" ]] || die "aborted"
	remote "docker compose down --remove-orphans"
	# The data dirs are owned by container users, so delete from a container
	# (works for root and docker-group users alike); the dirs themselves and
	# their ownership stay.
	remote "docker run --rm --entrypoint sh -v '$REMOTE_DIR':/d docker.io/library/postgres:16-alpine \
		-c 'find /d/pgdata /d/minio /d/staging -mindepth 1 -delete'"
	log "wiped. make vps-deploy starts with an empty database and bucket."
}

cmd_device() {
	local cmd=${DEVICE_CMD:-} dev=${DEVICE:-} ttl=${TTL:-} st=${STATUS:-} args=""
	case $cmd in
	approve | reject | revoke | issue | access-token)
		# Same rule as metadata device_id; also keeps the remote command safe.
		[[ $dev =~ ^[a-z0-9][a-z0-9-]{2,63}$ ]] || die "DEVICE='$dev' must match ^[a-z0-9][a-z0-9-]{2,63}$"
		args="-device $dev"
		if [[ $cmd == access-token && -n $ttl ]]; then
			[[ $ttl =~ ^[0-9]+(ns|us|ms|s|m|h)([0-9]+(ns|us|ms|s|m|h))*$ ]] || die "TTL='$ttl' is not a Go duration (e.g. 24h)"
			args+=" -ttl $ttl"
		fi
		;;
	list)
		if [[ -n $st ]]; then
			[[ $st =~ ^(pending|active|rejected|revoked)$ ]] || die "STATUS must be pending, active, rejected or revoked"
			args="-status $st"
		fi
		;;
	*) die "DEVICE_CMD must be approve, reject, revoke, issue, access-token or list" ;;
	esac
	remote "docker compose exec -T server /devicectl $cmd $args"
}

case ${1:-} in
deploy) cmd_deploy ;;
device) cmd_device ;;
status) cmd_status ;;
logs) cmd_logs ;;
down) cmd_down ;;
wipe) cmd_wipe ;;
*) echo "usage: $0 deploy|status|logs|down|wipe|device (VPS=user@host)" >&2; exit 2 ;;
esac
