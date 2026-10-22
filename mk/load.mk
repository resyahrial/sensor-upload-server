# Load tests (requirements NFR-2/NFR-3, task T24); included by the root
# Makefile via `-include mk/*.mk`. Builds cmd/server, runs it as a
# subprocess against a Postgres testcontainer and measures its RSS.
#
#   make load-test                              # rest, full config (50 x 200MiB, 200 in progress)
#   make load-test LOAD_PROTOCOL=rest-http3
#   make load-test LOAD_PROTOCOL=grpc           # /grpc prefix on the TLS listener (ServeHTTP)
#   make load-test LOAD_PROTOCOL=grpc-native    # native gRPC listener (GRPC_NATIVE_ADDR)
#   make load-test-all                          # every protocol in LOAD_PROTOCOLS in turn
#   make load-test LOAD_RUN=NFR3 LOAD_INPROGRESS=100
#
# Knobs (environment of test/load, empty = default): LOAD_PROTOCOL,
# LOAD_FILES, LOAD_FILE_SIZE, LOAD_CHUNK, LOAD_RSS_LIMIT, LOAD_INPROGRESS,
# LOAD_INPROGRESS_SIZE, LOAD_HOLD, LOAD_SERVER_PROCS, LOAD_DIR. NFR-2 needs
# about 2 x LOAD_FILES x LOAD_FILE_SIZE + 4 GiB free in LOAD_DIR.

LOAD_PROTOCOL  ?= rest
LOAD_PROTOCOLS ?= rest rest-http3 grpc grpc-native
LOAD_RUN       ?= .
LOAD_TIMEOUT   ?= 90m

LOAD_VARS = LOAD_FILES LOAD_FILE_SIZE LOAD_CHUNK LOAD_RSS_LIMIT LOAD_INPROGRESS \
	LOAD_INPROGRESS_SIZE LOAD_HOLD LOAD_SERVER_PROCS LOAD_DIR
LOAD_ENV = DOCKER_HOST=$(DOCKER_HOST) TESTCONTAINERS_RYUK_DISABLED=$(TESTCONTAINERS_RYUK_DISABLED) \
	$(foreach v,$(LOAD_VARS),$(if $($(v)),$(v)='$($(v))'))

.PHONY: load-test load-test-all lint-load

## load-test: NFR-2 + NFR-3 load tests for LOAD_PROTOCOL.
load-test:
	$(LOAD_ENV) LOAD_PROTOCOL=$(LOAD_PROTOCOL) \
		$(GO) test -tags load -count=1 -v -timeout $(LOAD_TIMEOUT) -run '$(LOAD_RUN)' ./test/load/

## load-test-all: load-test for every protocol in LOAD_PROTOCOLS.
load-test-all:
	@rc=0; for p in $(LOAD_PROTOCOLS); do \
		$(MAKE) --no-print-directory load-test LOAD_PROTOCOL=$$p || rc=1; \
	done; exit $$rc

## lint-load: vet + staticcheck for the load-tagged files (make lint does not
## build them).
lint-load:
	$(GO) vet -tags load ./test/load/
	$(STATICCHECK) -tags load ./test/load/
