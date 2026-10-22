#!/usr/bin/env bash
# Protocol experiment driver (requirements §12, tasks/plan.md "Experiment scope").
#
#   deploy/netem/experiment.sh          run every missing cell of the matrix
#   deploy/netem/experiment.sh run ID   run one cell (used internally via xargs)
#
# Matrix: PROTOCOLS × PROFILES × SIZES × CHUNKS × REPS. Each run uploads a
# fresh file (deterministic random base bytes + the run id, so every run has a
# unique sha256) with a fresh uploader state DB, inside its own netem
# container (own network namespace), and leaves results/runs/ID.csv:
#   run_id,profile,rep,exit_code,<uploader §11 report columns>
# A run whose CSV exists is skipped, so the driver can be killed and
# restarted. A run longer than RUN_TIMEOUT gets result=timeout. A run that
# ends without a report (e.g. server unreachable at start) leaves no CSV and
# is retried next time; see results/logs/ID.log.
#
# Environment (defaults in brackets):
#   PROTOCOLS [rest rest-http3 grpc grpc-native]  PROFILES [good-4g weak-4g leo-sat very-poor]
#   SIZES [50KB 2MB 20MB]  CHUNKS [64KB 256KB]  REPS [5]   (KB/MB = KiB/MiB)
#   EXPERIMENT_PARALLEL [4]  RUN_TIMEOUT seconds [7200]  RESULTS_DIR [<repo>/results]
#   SERVER_URL [https://server:8443]  GRPC_NATIVE_URL [server:9090]
#   TOKEN (device JWT) [$UPLOADER_TOKEN]  DEVICE [netem-0001]  CA_FILE (host path; empty = -insecure)
#   UPLOADER_ARGS [-retries 100000 -backoff-max 30s]  (gRPC runs use the uploader's
#                 default -grpc-window 1; add "-grpc-window N" here only with the
#                 server's GRPC_ACK_EVERY_N <= N)
#   NETEM_IMAGE [sensor-uploader-netem]  NETEM_NETWORK [deploy_default]  CONTAINER_RT [docker|podman]
#   UPLOADER_CMD  test hook: run this command on the host instead of the
#                 netem container (no shaping), with the same uploader args.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
self=$here/$(basename "$0")
root=$(cd "$here/../.." && pwd)

export PROTOCOLS=${PROTOCOLS:-"rest rest-http3 grpc grpc-native"}
export PROFILES=${PROFILES:-"good-4g weak-4g leo-sat very-poor"}
export SIZES=${SIZES:-"50KB 2MB 20MB"}
export CHUNKS=${CHUNKS:-"64KB 256KB"}
export REPS=${REPS:-5}
export EXPERIMENT_PARALLEL=${EXPERIMENT_PARALLEL:-4}
export RUN_TIMEOUT=${RUN_TIMEOUT:-7200}
export RESULTS_DIR=${RESULTS_DIR:-$root/results}
export SERVER_URL=${SERVER_URL:-https://server:8443}
export GRPC_NATIVE_URL=${GRPC_NATIVE_URL:-server:9090}
export UPLOADER_TOKEN=${TOKEN:-${UPLOADER_TOKEN:-}}
export DEVICE=${DEVICE:-netem-0001}
export CA_FILE=${CA_FILE:-}
export UPLOADER_ARGS=${UPLOADER_ARGS:-"-retries 100000 -backoff-max 30s"}
export NETEM_IMAGE=${NETEM_IMAGE:-sensor-uploader-netem}
export NETEM_NETWORK=${NETEM_NETWORK:-deploy_default}
export UPLOADER_CMD=${UPLOADER_CMD:-}
if [[ -z ${CONTAINER_RT:-} ]]; then
	CONTAINER_RT=$(command -v docker || command -v podman || echo docker)
fi
export CONTAINER_RT
if [[ -z ${TIMEOUT_BIN:-} ]]; then
	TIMEOUT_BIN=$(command -v timeout || command -v gtimeout || true)
fi
export TIMEOUT_BIN

log() { echo "experiment[$(date +%H:%M:%S)] $*" >&2; }
die() {
	log "error: $*"
	exit 1
}

# bytes 64KB → 65536, 2MB → 2097152.
bytes() {
	local n=${1%[KM]B}
	case $1 in
	*KB) echo $((n * 1024)) ;;
	*MB) echo $((n * 1024 * 1024)) ;;
	*) die "bad size $1 (want NKB or NMB)" ;;
	esac
}

# Deterministic pseudo-random bytes: AES-128-CTR keystream with a fixed key.
gen_samples() {
	local s n f
	mkdir -p "$RESULTS_DIR/samples"
	for s in $SIZES; do
		n=$(bytes "$s")
		f=$RESULTS_DIR/samples/$s.bin
		if [[ -f $f && $(wc -c <"$f" | tr -d ' ') -eq $n ]]; then
			continue
		fi
		log "generating $f ($n bytes)"
		{ openssl enc -aes-128-ctr -nosalt -K 0123456789abcdef0123456789abcdef \
			-iv 00000000000000000000000000000000 </dev/zero 2>/dev/null || true; } |
			head -c "$n" >"$f.tmp"
		mv "$f.tmp" "$f"
	done
}

csv_header="run_id,profile,rep,exit_code,protocol,file,size,chunk,duration_ms,attempts,resumes,bytes_sent_total,bytes_resent,result"

# run_one ID: one upload; writes $RESULTS_DIR/runs/ID.csv when it has a result.
run_one() {
	local id=$1 proto profile size chunk rep
	IFS=_ read -r proto profile size chunk rep <<<"$id"
	local n c work out logf base url
	n=$(bytes "$size")
	c=$(bytes "$chunk")
	work=$RESULTS_DIR/work/$id
	out=$RESULTS_DIR/runs/$id.csv
	logf=$RESULTS_DIR/logs/$id.log
	rm -rf "$work"
	mkdir -p "$work"

	# Unique content per run: base bytes with the last 64 replaced by the id.
	{
		head -c $((n - 64)) "$RESULTS_DIR/samples/$size.bin"
		printf '%-64s' "$id"
	} >"$work/$id.flac"

	url=$SERVER_URL
	[[ $proto == grpc-native ]] && url=$GRPC_NATIVE_URL
	local -a cmd tls
	if [[ -n $UPLOADER_CMD ]]; then
		base=$RESULTS_DIR
		tls=(-insecure)
		[[ -n $CA_FILE ]] && tls=(-ca "$CA_FILE")
		# shellcheck disable=SC2206 # UPLOADER_CMD may carry arguments
		cmd=($UPLOADER_CMD)
	else
		base=/results
		local name=netem-$id
		"$CONTAINER_RT" rm -f "$name" >/dev/null 2>&1 || true
		cmd=("$CONTAINER_RT" run --rm --name "$name" --label netem-experiment=1
			--cap-add NET_ADMIN --network "$NETEM_NETWORK"
			-e UPLOADER_TOKEN -e "OUTAGE_SEED=${rep#r}" -v "$RESULTS_DIR:/results")
		tls=(-insecure)
		if [[ -n $CA_FILE ]]; then
			cmd+=(-v "$CA_FILE:/certs/ca.pem:ro")
			tls=(-ca /certs/ca.pem)
		fi
		cmd+=("$NETEM_IMAGE" "$profile" --)
	fi
	# shellcheck disable=SC2206 # UPLOADER_ARGS is a list of flags
	cmd+=(-protocol "$proto" -url "$url" -device "$DEVICE" -chunk "$c"
		-file "$base/work/$id/$id.flac" -state "$base/work/$id/uploader.db"
		-report "$base/work/$id/report.csv" "${tls[@]}" $UPLOADER_ARGS)

	log "start $id"
	local start rc=0 tpid
	start=$(date +%s)
	"$TIMEOUT_BIN" -k 60 "$RUN_TIMEOUT" "${cmd[@]}" >"$logf" 2>&1 &
	tpid=$!
	# Driver interrupted: stop the run (timeout forwards the signal) and
	# leave no CSV, so it is redone next time.
	trap 'kill -TERM "$tpid" 2>/dev/null || true; wait "$tpid" 2>/dev/null || true; log "interrupted $id"; exit 143' TERM INT
	wait "$tpid" || rc=$?
	trap - TERM INT

	local row=""
	[[ -f $work/report.csv ]] && row=$(sed -n 2p "$work/report.csv")
	if [[ $rc -eq 124 || $rc -eq 137 ]]; then
		if [[ -n $row ]]; then
			row=${row%,*},timeout
		else
			row="$proto,$base/work/$id/$id.flac,$n,$c,$((($(date +%s) - start) * 1000)),0,0,0,0,timeout"
		fi
	elif [[ -z $row ]]; then
		log "no report for $id (exit $rc), will retry on the next run; see $logf"
		return 0
	fi
	printf '%s\n%s,%s,%s,%s,%s\n' "$csv_header" "$id" "$profile" "${rep#r}" "$rc" "$row" >"$out.tmp"
	mv "$out.tmp" "$out"
	rm -rf "$work"
	log "done $id: ${row##*,} (exit $rc)"
}

# stop_runs XARGS_PID: stop xargs and every running "run" (each stops its
# uploader/container), and wait until they are gone.
stop_runs() {
	local kids
	log "stopping"
	kids=$(pgrep -P "$1" || true)
	local k
	for k in "$1" $kids; do
		kill -TERM "$k" 2>/dev/null || true
	done
	wait "$1" 2>/dev/null || true
	for k in $kids; do
		while kill -0 "$k" 2>/dev/null; do sleep 1; done
	done
	log "stopped; unfinished runs are redone on the next start"
}

main() {
	[[ -n $TIMEOUT_BIN ]] || die "need GNU timeout (coreutils; 'gtimeout' on macOS)"
	command -v openssl >/dev/null || die "need openssl to generate samples"
	if [[ -z $UPLOADER_CMD ]]; then
		"$CONTAINER_RT" image inspect "$NETEM_IMAGE" >/dev/null 2>&1 ||
			die "image $NETEM_IMAGE missing: docker build -f deploy/netem/Dockerfile.uploader -t $NETEM_IMAGE ."
		"$CONTAINER_RT" network inspect "$NETEM_NETWORK" >/dev/null 2>&1 ||
			die "network $NETEM_NETWORK missing (start the main compose project or set NETEM_NETWORK)"
	fi
	mkdir -p "$RESULTS_DIR/runs" "$RESULTS_DIR/logs" "$RESULTS_DIR/work"
	gen_samples

	local -a todo=()
	local rep size profile proto chunk id total=0
	for ((rep = 1; rep <= REPS; rep++)); do
		for size in $SIZES; do
			for profile in $PROFILES; do
				[[ -x $here/profiles/$profile.sh ]] || die "unknown profile $profile"
				for proto in $PROTOCOLS; do
					for chunk in $CHUNKS; do
						id=${proto}_${profile}_${size}_${chunk}_r${rep}
						total=$((total + 1))
						[[ -f $RESULTS_DIR/runs/$id.csv ]] || todo+=("$id")
					done
				done
			done
		done
	done
	log "$total runs in the matrix, ${#todo[@]} to do, parallel $EXPERIMENT_PARALLEL"
	[[ ${#todo[@]} -gt 0 ]] || return 0

	printf '%s\n' "${todo[@]}" | xargs -P "$EXPERIMENT_PARALLEL" -n 1 "$self" run &
	local xpid=$!
	trap 'stop_runs "$xpid"; exit 130' TERM INT
	local rc=0
	wait "$xpid" || rc=$?
	trap - TERM INT
	local left=0
	for id in "${todo[@]}"; do
		[[ -f $RESULTS_DIR/runs/$id.csv ]] || left=$((left + 1))
	done
	log "finished: $((${#todo[@]} - left)) of ${#todo[@]} runs recorded, $left without result"
	[[ $rc -eq 0 && $left -eq 0 ]]
}

case ${1:-} in
run)
	[[ $# -eq 2 ]] || die "usage: $0 run ID"
	run_one "$2"
	;;
"") main ;;
*) die "usage: $0 [run ID]" ;;
esac
