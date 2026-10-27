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
# HOST_BIND only (loopback by default: the dev stack uses default Postgres and
# MinIO credentials).
HOST_BIND         ?= 127.0.0.1
HOST_HTTPS_PORT   ?= 18443
HOST_GRPC_PORT    ?= 19090
HOST_METRICS_PORT ?= 19100
export HOST_BIND HOST_HTTPS_PORT HOST_GRPC_PORT HOST_METRICS_PORT

.PHONY: build test test-integration up down devcert lint device-issue device-token device-approve device-list device-batch device-require

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

devcert: $(CERT_DIR)/server.crt $(CERT_DIR)/jwt.key

# Self-signed TLS certificate for local development (dev only; key is world-readable
# so the distroless nonroot container user can read the bind mount).
$(CERT_DIR)/server.crt:
	mkdir -p $(CERT_DIR)
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 365 \
		-subj "/CN=localhost" \
		-addext "subjectAltName=DNS:localhost,DNS:server,IP:127.0.0.1,IP:::1" \
		-keyout $(CERT_DIR)/server.key -out $@
	chmod 644 $(CERT_DIR)/server.key

# ES256 (P-256, PKCS #8) access-token signing key for the dev stack
# (JWT_SIGNING_KEY_FILE). Dev only: world-readable like server.key.
$(CERT_DIR)/jwt.key:
	mkdir -p $(CERT_DIR)
	openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -pkeyopt ec_param_enc:named_curve -out $@
	chmod 644 $@

up: devcert
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --build

down:
	$(COMPOSE) -f $(COMPOSE_FILE) down

# Device admin in the running dev stack (`make up`) through /devicectl:
#   make device-batch DEVICE=netem-0001     # pre-register (batch import) DEVICE
#   make device-approve DEVICE=netem-0001   # approve a pending device
#   make device-list [STATUS=active]
#   make -s device-issue DEVICE=netem-0001 > tokens.json   # token pair (uploader -refresh-token-file)
#   TOKEN=$$(make -s device-token DEVICE=netem-0001 [TTL=24h])   # access JWT only (uploader -token)
# Revoke / reject: $(COMPOSE) -f $(COMPOSE_FILE) exec -T server /devicectl revoke -device ID
DEVICE ?=
TTL    ?=
STATUS ?=
DEVICECTL = $(COMPOSE) -f $(COMPOSE_FILE) exec -T server /devicectl
device-require:
	@test -n '$(DEVICE)' || { echo "set DEVICE=... (e.g. make device-token DEVICE=netem-0001)" >&2; exit 2; }

## device-issue: allow-list and activate DEVICE, print a fresh token pair JSON.
device-issue: device-require
	@$(DEVICECTL) issue -device '$(DEVICE)'

## device-token: allow-list and activate DEVICE, print only an access JWT (TTL default: ACCESS_TOKEN_TTL).
device-token: device-require
	@$(DEVICECTL) access-token -device '$(DEVICE)' $(if $(TTL),-ttl '$(TTL)')

device-approve: device-require
	@$(DEVICECTL) approve -device '$(DEVICE)'

device-batch: device-require
	@$(DEVICECTL) batch-import -device '$(DEVICE)'

device-list:
	@$(DEVICECTL) list $(if $(STATUS),-status '$(STATUS)')

# Extra targets (proto, experiment, netem, ...) live in mk/*.mk.
-include mk/*.mk
