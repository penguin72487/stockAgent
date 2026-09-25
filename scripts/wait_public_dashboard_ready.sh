#!/usr/bin/env bash
set -euo pipefail

# Type=simple reports active before Python has bound its loopback socket.
# Keep systemd's start job pending until the actual read-only gateway responds.
deadline=$((SECONDS + 60))
while ((SECONDS < deadline)); do
    if /usr/bin/curl --silent --fail --output /dev/null --max-time 2 \
        http://127.0.0.1:8770/healthz; then
        exit 0
    fi
    sleep 1
done
echo "public-dashboard gateway readiness timed out" >&2
exit 1
