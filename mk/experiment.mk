# Protocol experiment (requirements §12, tasks T22/T23); included by the root
# Makefile via `-include mk/*.mk`. Needs the server stack running on the
# NETEM_NETWORK container network (default: the main compose project's).
#
#   make experiment-smoke TOKEN=$(go run ./cmd/devtoken ...) CA_FILE=certs/ca.pem
#   make experiment SERVER_URL=https://server:8443 GRPC_NATIVE_URL=server:9090 TOKEN=... CA_FILE=...
#   make summary
#
# Other knobs (environment of deploy/netem/experiment.sh): DEVICE,
# EXPERIMENT_PARALLEL, RUN_TIMEOUT, RESULTS_DIR, NETEM_NETWORK, UPLOADER_ARGS,
# UPLOADER_CMD (test hook). Empty values fall back to the script's defaults.

CONTAINER_RT ?= $(shell command -v docker || command -v podman)
NETEM_IMAGE ?= sensor-uploader-netem
RESULTS_DIR ?= results
EXPERIMENT ?= deploy/netem/experiment.sh
SUMMARIZE = go run ./cmd/summarize -runs $(RESULTS_DIR)/runs -out $(RESULTS_DIR)/summary.csv

EXPERIMENT_ENV = SERVER_URL='$(SERVER_URL)' GRPC_NATIVE_URL='$(GRPC_NATIVE_URL)' \
	TOKEN='$(TOKEN)' CA_FILE='$(if $(CA_FILE),$(abspath $(CA_FILE)))' \
	RESULTS_DIR='$(abspath $(RESULTS_DIR))' NETEM_IMAGE='$(NETEM_IMAGE)' \
	CONTAINER_RT='$(CONTAINER_RT)'

.PHONY: netem-image experiment experiment-smoke summary

## netem-image: build the uploader image with tc/netem (deploy/netem/Dockerfile.uploader).
netem-image:
	$(CONTAINER_RT) build -f deploy/netem/Dockerfile.uploader -t $(NETEM_IMAGE) .

## experiment: full matrix (480 runs; resumable), then results/summary.csv.
experiment: $(if $(UPLOADER_CMD),,netem-image)
	$(EXPERIMENT_ENV) $(EXPERIMENT); rc=$$?; $(SUMMARIZE); exit $$rc

## experiment-smoke: 1 rep, all protocols, good-4g + very-poor, 50KB + 2MB, 64KB chunks.
experiment-smoke: $(if $(UPLOADER_CMD),,netem-image)
	$(EXPERIMENT_ENV) PROFILES='good-4g very-poor' SIZES='50KB 2MB' CHUNKS='64KB' REPS=1 \
		$(EXPERIMENT); rc=$$?; $(SUMMARIZE); exit $$rc

## summary: merge results/runs/*.csv into results/summary.csv.
summary:
	$(SUMMARIZE)
