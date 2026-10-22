#!/usr/bin/env bash
# Profile very-poor (requirements §12): 64 kbit, 800 ms ± 200, 10% loss, 5 s
# down every ~30 s (period 30 s ± 5 s between outage starts).
# Usage: very-poor.sh apply|outages|down|up|clear   (needs CAP_NET_ADMIN)
set -euo pipefail
NETEM_LINK="rate 64kbit delay 800ms 200ms"
NETEM_LOSS="10%"
OUTAGE_EVERY=30
OUTAGE_JITTER=5
OUTAGE_FOR=5
# shellcheck source-path=SCRIPTDIR/.. source=netem-lib.sh
. "$(dirname "$0")/../netem-lib.sh"
