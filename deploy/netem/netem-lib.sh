# shellcheck shell=bash
# Shared netem logic, sourced by profiles/*.sh. A profile sets:
#   NETEM_LINK  netem options without loss, e.g. "rate 5mbit delay 50ms 10ms"
#   NETEM_LOSS  loss percentage, e.g. "0.1%"
#   OUTAGE_EVERY / OUTAGE_JITTER / OUTAGE_FOR  repeating link-down events, in
#               seconds (period between outage starts, ± jitter, duration)
#   LINK_OFF_AFTER  seconds until the link goes down for good
# and then sources this file, which dispatches on the first argument:
#   apply    replace the root qdisc of $NETEM_DEV (default eth0) with netem
#   outages  run the link-down schedule in the foreground (forever)
#   down|up  one link-down / link-up step
#   clear    remove the root qdisc
#
# Shaping is egress only: in the uploader's container it covers the upload
# payload; delay therefore adds to the RTT once (see docs/decisions.md).
# Link-down = netem "loss 100%" on the same qdisc, so routes and addresses
# stay intact and "up" restores the link exactly.

NETEM_DEV=${NETEM_DEV:-eth0}

netem_log() { echo "netem[$(date -u +%H:%M:%S)] $*" >&2; }

netem_apply() {
	# Podman networks can use a 65520-byte MTU; at 64 kbit one such packet
	# takes 8 s and one loss drops 64 KiB. Use a real link's MTU instead (TCP
	# advertises the matching MSS, so the peer sends small packets too).
	ip link set dev "$NETEM_DEV" mtu "${NETEM_MTU:-1500}"
	# Offloads would hand netem 64 KiB GSO super-packets, so one "lost packet"
	# would drop dozens of wire packets. Best effort: not every veth allows it.
	ethtool -K "$NETEM_DEV" tso off gso off gro off 2>/dev/null ||
		netem_log "warning: could not disable offloads on $NETEM_DEV"
	# shellcheck disable=SC2086 # NETEM_LINK is a list of tc words
	tc qdisc replace dev "$NETEM_DEV" root netem $NETEM_LINK loss "$NETEM_LOSS"
	netem_log "applied: $(tc qdisc show dev "$NETEM_DEV" root)"
}

netem_down() {
	# shellcheck disable=SC2086
	tc qdisc change dev "$NETEM_DEV" root netem $NETEM_LINK loss 100%
	netem_log "link down"
}

netem_up() {
	# shellcheck disable=SC2086
	tc qdisc change dev "$NETEM_DEV" root netem $NETEM_LINK loss "$NETEM_LOSS"
	netem_log "link up"
}

netem_clear() {
	tc qdisc del dev "$NETEM_DEV" root 2>/dev/null || true
}

netem_outages() {
	# Same seed → same outage schedule (e.g. per repetition across protocols).
	RANDOM=${OUTAGE_SEED:-$$}
	if [[ -n ${LINK_OFF_AFTER:-} ]]; then
		sleep "$LINK_OFF_AFTER"
		netem_down
		return 0
	fi
	[[ -n ${OUTAGE_EVERY:-} ]] || return 0
	local gap
	while :; do
		gap=$((OUTAGE_EVERY - OUTAGE_FOR - OUTAGE_JITTER + RANDOM % (2 * OUTAGE_JITTER + 1)))
		sleep "$gap"
		netem_down
		sleep "$OUTAGE_FOR"
		netem_up
	done
}

case ${1:-} in
apply) netem_apply ;;
outages) netem_outages ;;
down) netem_down ;;
up) netem_up ;;
clear) netem_clear ;;
*)
	echo "usage: $0 apply|outages|down|up|clear" >&2
	exit 2
	;;
esac
