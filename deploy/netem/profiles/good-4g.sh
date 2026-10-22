#!/usr/bin/env bash
# Profile good-4g (requirements §12): 5 Mbit, 50 ms ± 10, 0.1% loss, no
# link-down events.
# Usage: good-4g.sh apply|outages|down|up|clear   (needs CAP_NET_ADMIN)
set -euo pipefail
NETEM_LINK="rate 5mbit delay 50ms 10ms"
NETEM_LOSS="0.1%"
# shellcheck source-path=SCRIPTDIR/.. source=netem-lib.sh
. "$(dirname "$0")/../netem-lib.sh"
