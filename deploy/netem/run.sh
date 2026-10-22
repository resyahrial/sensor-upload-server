#!/usr/bin/env bash
# Entrypoint of the netem uploader image.
#
#   run.sh PROFILE -- <uploader args>     e.g. run.sh weak-4g -- -protocol grpc -file ...
#   run.sh PROFILE -- <command> [args]    any other command (first word not "-…"),
#                                         e.g. run.sh good-4g -- ping -c 5 server
#
# Applies profiles/PROFILE.sh to eth0 (needs CAP_NET_ADMIN), runs the
# profile's link-down schedule in the background, runs the uploader, then
# stops the schedule and removes the qdisc. SIGTERM/SIGINT are forwarded to
# the uploader (which then writes its report) and the exit status is the
# uploader's.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
if [[ $# -lt 1 ]]; then
	echo "usage: $0 PROFILE -- <uploader args | command>" >&2
	exit 2
fi
profile=$1
shift
[[ ${1:-} == "--" ]] && shift
script=$here/profiles/$profile.sh
if [[ ! -x $script ]]; then
	echo "run.sh: unknown profile $profile; have:" >&2
	for p in "$here"/profiles/*.sh; do basename "$p" .sh >&2; done
	exit 2
fi
if [[ $# -eq 0 || ${1:0:1} == "-" ]]; then
	set -- uploader "$@"
fi

"$script" apply
"$script" outages &
outages=$!
child=""

# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
	kill "$outages" 2>/dev/null || true
	wait "$outages" 2>/dev/null || true
	"$script" clear
}
trap cleanup EXIT
trap '[[ -n $child ]] && kill -TERM "$child" 2>/dev/null' TERM INT

"$@" &
child=$!
rc=0
wait "$child" || rc=$?
# A trapped signal interrupts wait; wait again so the uploader can finish
# writing its report.
while kill -0 "$child" 2>/dev/null; do
	rc=0
	wait "$child" || rc=$?
done
exit "$rc"
