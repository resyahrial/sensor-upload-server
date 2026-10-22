# Single-VPS deployment (deploy/vps, README "Deploy to a VPS"); included by
# the root Makefile via `-include mk/*.mk`.
#
#   make vps-secrets   VPS_IP=203.0.113.10 [VPS_SAN=IP:10.0.0.5,DNS:x] [VPS_SECRETS_ARGS=--renew-cert]
#   make vps-bootstrap VPS=root@203.0.113.10 [VPS_DEPLOY_USER=ubuntu]
#   make vps-deploy    VPS=root@203.0.113.10 [VPS_ARCH=arm64]
#   make vps-status | vps-logs [SERVICE=server FOLLOW=1] | vps-down | vps-wipe  VPS=...
#   make vps-tunnel    VPS=...   # localhost:9100 admin/metrics, localhost:9001 MinIO console
#   TOKEN=$$(make -s vps-token VPS_DEVICE=dev-1 [VPS_TOKEN_TTL=24h])
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
VPS_DEVICE      ?=
VPS_TOKEN_TTL   ?= 24h
VPS_QUIC_SYSCTL ?= 1
VPS_SSH_PORT    ?= 22
VPS_SSH_OPTS    ?=

VPS_DEPLOY = VPS='$(VPS)' VPS_IP='$(VPS_IP)' VPS_ARCH='$(VPS_ARCH)' \
	SECRETS_DIR='$(abspath $(VPS_SECRETS_DIR))' REMOTE_DIR='$(VPS_REMOTE_DIR)' \
	SSH_OPTS='$(VPS_SSH_OPTS)' GO='$(GO)' deploy/vps/deploy.sh

.PHONY: vps-secrets vps-bootstrap vps-deploy vps-status vps-logs vps-down vps-wipe vps-token vps-tunnel

vps-require-%:
	@test -n '$($*)' || { echo "set $*=... (see mk/vps.mk)" >&2; exit 2; }

## vps-secrets: CA + server cert (SAN = VPS_IP), ES256 JWT keys, .env (operator machine).
vps-secrets: vps-require-VPS_IP
	deploy/vps/gen-secrets.sh --ip '$(VPS_IP)' $(if $(VPS_SAN),--san '$(VPS_SAN)') \
		--out '$(VPS_SECRETS_DIR)' $(VPS_SECRETS_ARGS)

## vps-bootstrap: one-time VPS setup as root (Docker, /srv/sensor-upload, ufw, QUIC sysctls).
vps-bootstrap: vps-require-VPS
	ssh $(VPS_SSH_OPTS) '$(VPS)' "DEPLOY_USER='$(VPS_DEPLOY_USER)' DATA_DIR='$(VPS_REMOTE_DIR)' \
		SSH_PORT='$(VPS_SSH_PORT)' QUIC_SYSCTL='$(VPS_QUIC_SYSCTL)' bash -s" < deploy/vps/bootstrap.sh

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

## vps-token: device JWT (ES256, signed locally with $(VPS_SECRETS_DIR)/jwt.key) on stdout.
vps-token: vps-require-VPS_DEVICE
	@$(GO) run ./cmd/devtoken -alg ES256 -key '$(VPS_SECRETS_DIR)/jwt.key' \
		-device '$(VPS_DEVICE)' -ttl '$(VPS_TOKEN_TTL)'

## vps-tunnel: forward the loopback-only admin (9100) and MinIO console (9001) ports.
vps-tunnel: vps-require-VPS
	@echo "admin: http://localhost:9100/readyz  MinIO console: http://localhost:9001 (Ctrl-C to stop)"
	ssh $(VPS_SSH_OPTS) -N -L 9100:127.0.0.1:9100 -L 9001:127.0.0.1:9001 '$(VPS)'
