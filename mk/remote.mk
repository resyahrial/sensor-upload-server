# Manual remote e2e suite (test/remote, build tag remote); included by the
# root Makefile via `-include mk/*.mk`. Runs against an already running
# server configured by REMOTE_* environment variables (see README "Remote
# e2e tests"); never part of `make test` or `make test-integration`.
#
#   REMOTE_URL=https://203.0.113.7:8443 REMOTE_CA_FILE=certs/server.crt \
#   REMOTE_TOKEN=$$(make -s vps-device-key VPS=... VPS_DEVICE=remote-e2e) \
#   [REMOTE_OTHER_TOKEN=<token of a second device>] make e2e-remote
#   make e2e-remote RUN=TestRemoteUploadREST       # one test (anchored: ^(RUN)$)
#   make e2e-remote RUN='TestRemoteUpload.*'       # a group
#   make e2e-remote-list                           # list the test names

RUN            ?=
REMOTE_TIMEOUT ?= 15m

.PHONY: e2e-remote e2e-remote-list lint-remote

## e2e-remote: run the remote suite (all tests, or RUN=<regexp>).
e2e-remote:
	$(GO) test -tags remote -count=1 -v -timeout $(REMOTE_TIMEOUT) $(if $(RUN),-run '^($(RUN))$$') ./test/remote/

## e2e-remote-list: list the remote test names.
e2e-remote-list:
	@$(GO) test -tags remote -list '.*' ./test/remote/ | grep '^Test'

## lint-remote: vet + staticcheck for the remote-tagged files (also run by make lint).
lint-remote:
	$(GO) vet -tags remote ./test/remote/
	$(STATICCHECK) -tags remote ./test/remote/
