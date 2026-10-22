#!/usr/bin/env bash
# Profile leo-sat (requirements §12): 2 Mbit, 60 ms ± 20, 1% loss, link off
# for good after 10 min.
# Usage: leo-sat.sh apply|outages|down|up|clear   (needs CAP_NET_ADMIN)
set -euo pipefail
NETEM_LINK="rate 2mbit delay 60ms 20ms"
NETEM_LOSS="1%"
LINK_OFF_AFTER=${LINK_OFF_AFTER:-600}
# shellcheck source-path=SCRIPTDIR/.. source=netem-lib.sh
. "$(dirname "$0")/../netem-lib.sh"
