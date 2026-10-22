GO           ?= go
# Use the toolchain pinned in go.mod (go downloads it if needed), even when the
# user's go env says GOTOOLCHAIN=local. Override from the environment.
GOTOOLCHAIN  ?= auto
export GOTOOLCHAIN
BIN_DIR      ?= bin
CERT_DIR     ?= certs
STATICCHECK  ?= $(GO) run honnef.co/go/tools/cmd/staticcheck@v0.8.2
COMPOSE      ?= podman compose
COMPOSE_FILE ?= deploy/docker-compose.yml

# testcontainers-go talks to podman through its Docker-compatible socket.
DOCKER_HOST                  ?= unix:///var/folders/b2/x__nqytj5sq2bqytxpjhj8hh0000gn/T/podman/podman-machine-default-api.sock
TESTCONTAINERS_RYUK_DISABLED ?= true
# Packages tested in parallel by test-integration (each starts its own
# containers; the podman VM has 2 GB).
INTEGRATION_P ?= 2

# Host ports for `make up` (chosen to avoid common collisions), published on
# HOST_BIND only (loopback by default: the dev stack runs DEV_MODE + HS256).
HOST_BIND         ?= 127.0.0.1
HOST_HTTPS_PORT   ?= 18443
HOST_GRPC_PORT    ?= 19090
HOST_METRICS_PORT ?= 19100
export HOST_BIND HOST_HTTPS_PORT HOST_GRPC_PORT HOST_METRICS_PORT

.PHONY: build test test-integration up down devcert lint

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/ ./cmd/...

test:
	$(GO) test -cover ./...

test-integration:
	DOCKER_HOST=$(DOCKER_HOST) TESTCONTAINERS_RYUK_DISABLED=$(TESTCONTAINERS_RYUK_DISABLED) \
		$(GO) test -tags integration -count=1 -p $(INTEGRATION_P) -cover ./...

lint:
	$(GO) vet ./...
	$(GO) vet -tags integration ./...
	$(STATICCHECK) ./...
	$(STATICCHECK) -tags integration ./...
	$(GO) vet -tags remote ./test/remote/
	$(STATICCHECK) -tags remote ./test/remote/

devcert: $(CERT_DIR)/server.crt $(CERT_DIR)/jwt.pub $(CERT_DIR)/jwt-hmac.env

# Self-signed TLS certificate for local development (dev only; key is world-readable
# so the distroless nonroot container user can read the bind mount).
$(CERT_DIR)/server.crt:
	mkdir -p $(CERT_DIR)
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 365 \
		-subj "/CN=localhost" \
		-addext "subjectAltName=DNS:localhost,DNS:server,IP:127.0.0.1,IP:::1" \
		-keyout $(CERT_DIR)/server.key -out $@
	chmod 644 $(CERT_DIR)/server.key

# ES256 key pair for device JWTs (cmd/devtoken signs with jwt.key; the server verifies with jwt.pub).
$(CERT_DIR)/jwt.pub:
	mkdir -p $(CERT_DIR)
	openssl ecparam -name prime256v1 -genkey -noout -out $(CERT_DIR)/jwt.key
	openssl ec -in $(CERT_DIR)/jwt.key -pubout -out $@
	chmod 644 $(CERT_DIR)/jwt.key

# Random HS256 secret (32 bytes, hex) for the compose stack's dev tokens, as an
# env file read by deploy/docker-compose.yml (env_file). Owner-only: compose
# reads it on the host. Delete it to rotate.
$(CERT_DIR)/jwt-hmac.env:
	mkdir -p $(CERT_DIR)
	umask 077 && printf 'JWT_HMAC_SECRET=%s\n' "$$(openssl rand -hex 32)" > $@.tmp && mv $@.tmp $@

up: devcert
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --build

down:
	$(COMPOSE) -f $(COMPOSE_FILE) down

# Extra targets (proto, experiment, netem, ...) live in mk/*.mk.
-include mk/*.mk
