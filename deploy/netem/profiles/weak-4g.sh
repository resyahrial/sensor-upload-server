#!/usr/bin/env bash
# Profile weak-4g (requirements §12): 256 kbit, 300 ms ± 100, 3% loss, 10 s
# down every ~2 min (period 120 s ± 30 s between outage starts).
# Usage: weak-4g.sh apply|outages|down|up|clear   (needs CAP_NET_ADMIN)
set -euo pipefail
NETEM_LINK="rate 256kbit delay 300ms 100ms"
NETEM_LOSS="3%"
OUTAGE_EVERY=120
OUTAGE_JITTER=30
OUTAGE_FOR=10
# shellcheck source-path=SCRIPTDIR/.. source=netem-lib.sh
. "$(dirname "$0")/../netem-lib.sh"
