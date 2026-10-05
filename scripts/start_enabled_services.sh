#!/usr/bin/env bash
# Continue a dependency chain only after its owner's startup guard succeeds.
set -euo pipefail
for recovery_service in "$@"; do
    if [[ ! "$recovery_service" =~ ^(stockagent-[a-z0-9-]+\.service|syncthing@[a-zA-Z0-9._-]+\.service)$ ]]; then
        echo 'Unsupported startup dependency name' >&2
        exit 2
    fi
    recovery_service_state="$(systemctl is-enabled "$recovery_service" 2>/dev/null || true)"
    case "$recovery_service_state" in
        enabled|enabled-runtime)
            systemctl start --no-block "$recovery_service"
            ;;
        disabled|masked|masked-runtime|not-found|static|indirect)
            echo "Startup dependency deferred: $recovery_service ($recovery_service_state)"
            ;;
        *)
            echo "Cannot determine startup dependency state: $recovery_service" >&2
            exit 75
            ;;
    esac
done
