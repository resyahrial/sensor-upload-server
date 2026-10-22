#!/usr/bin/env bash
# Fake uploader for testing experiment.sh without a server:
#   UPLOADER_CMD=deploy/netem/testdata/stub-uploader.sh deploy/netem/experiment.sh
# Accepts the uploader flags it needs (-protocol -file -chunk -report), sleeps
# STUB_SLEEP seconds (default 1), then writes a §11 report row with
# result=ok. Runs whose file path matches STUB_HANG (a glob) sleep forever,
# to exercise the driver's timeout. On SIGTERM it writes result=interrupted,
# like the real uploader.
set -euo pipefail
proto="" file="" chunk="" report=""
while [[ $# -gt 0 ]]; do
	case $1 in
	-protocol) proto=$2 ;;
	-file) file=$2 ;;
	-chunk) chunk=$2 ;;
	-report) report=$2 ;;
	-insecure) shift && continue ;;
	esac
	shift 2
done
size=$(wc -c <"$file" | tr -d ' ')
start=$(date +%s)
write() {
	printf 'protocol,file,size,chunk,duration_ms,attempts,resumes,bytes_sent_total,bytes_resent,result\n' >"$report"
	printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$proto" "$file" "$size" "$chunk" \
		$((($(date +%s) - start) * 1000 + 7)) 2 1 $((size + chunk)) "$chunk" "$1" >>"$report"
}
trap 'kill "$sleeper" 2>/dev/null; write interrupted; exit 1' TERM INT
delay=${STUB_SLEEP:-1}
# shellcheck disable=SC2053 # STUB_HANG is a glob on purpose
[[ -n ${STUB_HANG:-} && $file == $STUB_HANG ]] && delay=100000
sleeper=""
sleep "$delay" &
sleeper=$!
wait "$sleeper"
write ok
