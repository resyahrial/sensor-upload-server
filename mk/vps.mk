# Single-VPS deployment (deploy/vps, README "Deploy to a VPS"); included by
# the root Makefile via `-include mk/*.mk`.
#
#   make vps-secrets   VPS_IP=203.0.113.10 [VPS_SAN=IP:10.0.0.5,DNS:x] [VPS_SECRETS_ARGS=--renew-cert]
#   make vps-bootstrap VPS=root@203.0.113.10 [VPS_DEPLOY_USER=ubuntu]
#   make vps-deploy    VPS=root@203.0.113.10 [VPS_ARCH=arm64]
#   make vps-status | vps-logs [SERVICE=server FOLLOW=1] | vps-down | vps-wipe  VPS=...
#   make vps-tunnel    VPS=...   # localhost:9100 admin/metrics, localhost:9001 MinIO console
#   make vps-device-batch   VPS=... VPS_DEVICE=dev-1   # register dev-1 (batch import)
#   make vps-device-approve VPS=... VPS_DEVICE=dev-1   # approve a pending device
#   make -s vps-device-issue VPS=... VPS_DEVICE=dev-1 > tokens.json   # token pair (uploader -refresh-token-file)
#   TOKEN=$$(make -s vps-device-token VPS=... VPS_DEVICE=dev-1 [TTL=24h])   # access JWT (uploader -token)
#   make vps-device-revoke VPS=... VPS_DEVICE=dev-1 | vps-device-list VPS=... [STATUS=active]
#   (VPS_DEVICE may also be given as DEVICE)
#
# VPS_SSH_OPTS (e.g. "-p 2222 -i ~/.ssh/vps") is passed to ssh/rsync.

VPS             ?=
VPS_IP          ?=
VPS_SAN         ?=
VPS_ARCH        ?= amd64
VPS_SECRETS_DIR ?= deploy/vps/secrets
VPS_SECRETS_ARGS ?=
VPS_DEPLOY_USER ?= root
VPS_REMOTE_DIR  ?= /srv/sensor-upload
VPS_DEVICE      ?= $(DEVICE)
VPS_QUIC_SYSCTL ?= 1
VPS_SSH_PORT    ?= 22
VPS_PUBLIC_PORT ?= 9000
VPS_SSH_OPTS    ?=

VPS_DEPLOY = VPS='$(VPS)' VPS_IP='$(VPS_IP)' VPS_ARCH='$(VPS_ARCH)' \
	SECRETS_DIR='$(abspath $(VPS_SECRETS_DIR))' REMOTE_DIR='$(VPS_REMOTE_DIR)' \
	SSH_OPTS='$(VPS_SSH_OPTS)' GO='$(GO)' deploy/vps/deploy.sh

.PHONY: vps-secrets vps-bootstrap vps-deploy vps-status vps-logs vps-down vps-wipe vps-tunnel \
	vps-device-batch vps-device-approve vps-device-reject vps-device-revoke vps-device-issue vps-device-token vps-device-list

vps-require-%:
	@test -n '$($*)' || { echo "set $*=... (see mk/vps.mk)" >&2; exit 2; }

## vps-secrets: CA + server cert (SAN = VPS_IP), JWT signing key, .env (operator machine).
vps-secrets: vps-require-VPS_IP
	deploy/vps/gen-secrets.sh --ip '$(VPS_IP)' $(if $(VPS_SAN),--san '$(VPS_SAN)') \
		--out '$(VPS_SECRETS_DIR)' $(VPS_SECRETS_ARGS)

## vps-bootstrap: one-time VPS setup as root (Docker, /srv/sensor-upload, ufw on VPS_PUBLIC_PORT, QUIC sysctls).
vps-bootstrap: vps-require-VPS
	ssh $(VPS_SSH_OPTS) '$(VPS)' "DEPLOY_USER='$(VPS_DEPLOY_USER)' DATA_DIR='$(VPS_REMOTE_DIR)' \
		SSH_PORT='$(VPS_SSH_PORT)' PUBLIC_PORT='$(VPS_PUBLIC_PORT)' QUIC_SYSCTL='$(VPS_QUIC_SYSCTL)' bash -s" < deploy/vps/bootstrap.sh

## vps-deploy: build linux/$(VPS_ARCH) image locally, copy it + compose + certs + .env, up -d, wait for /readyz.
vps-deploy: vps-require-VPS
	$(VPS_DEPLOY) deploy

vps-status: vps-require-VPS
	@$(VPS_DEPLOY) status

vps-logs: vps-require-VPS
	@SERVICE='$(SERVICE)' TAIL='$(TAIL)' FOLLOW='$(FOLLOW)' $(VPS_DEPLOY) logs

vps-down: vps-require-VPS
	$(VPS_DEPLOY) down

## vps-wipe: stop the stack and delete all data (asks for confirmation).
vps-wipe: vps-require-VPS
	@$(VPS_DEPLOY) wipe

# Device admin on the VPS: ssh + docker compose exec server /devicectl.
VPS_DEVICECTL = DEVICE='$(VPS_DEVICE)' TTL='$(TTL)' STATUS='$(STATUS)' $(VPS_DEPLOY) device

## vps-device-batch: register VPS_DEVICE (batch import, active).
vps-device-batch: vps-require-VPS vps-require-VPS_DEVICE
	@DEVICE_CMD=batch-import $(VPS_DEVICECTL)

## vps-device-approve / -reject / -revoke: change VPS_DEVICE's status.
vps-device-approve: vps-require-VPS vps-require-VPS_DEVICE
	@DEVICE_CMD=approve $(VPS_DEVICECTL)

vps-device-reject: vps-require-VPS vps-require-VPS_DEVICE
	@DEVICE_CMD=reject $(VPS_DEVICECTL)

vps-device-revoke: vps-require-VPS vps-require-VPS_DEVICE
	@DEVICE_CMD=revoke $(VPS_DEVICECTL)

## vps-device-issue: print a new token pair JSON for VPS_DEVICE (stdout).
vps-device-issue: vps-require-VPS vps-require-VPS_DEVICE
	@DEVICE_CMD=issue $(VPS_DEVICECTL)

## vps-device-token: print only an access JWT for VPS_DEVICE (stdout; TTL default: ACCESS_TOKEN_TTL).
vps-device-token: vps-require-VPS vps-require-VPS_DEVICE
	@DEVICE_CMD=access-token $(VPS_DEVICECTL)

## vps-device-list: devices and their status ([STATUS=pending|active|rejected|revoked]).
vps-device-list: vps-require-VPS
	@DEVICE_CMD=list $(VPS_DEVICECTL)

## vps-tunnel: forward the loopback-only admin (9100) and MinIO console (9001) ports.
vps-tunnel: vps-require-VPS
	@echo "admin: http://localhost:9100/readyz  MinIO console: http://localhost:9001 (Ctrl-C to stop)"
	ssh $(VPS_SSH_OPTS) -N -L 9100:127.0.0.1:9100 -L 9001:127.0.0.1:9001 '$(VPS)'
