#!/usr/bin/env bash
set -euo pipefail
news_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
news_service_user="${STOCKAGENT_SERVICE_USER:-$(stat -c '%U' "$news_repo_root")}"
news_service_group="$(id -gn "$news_service_user")"
news_service_home="$(getent passwd "$news_service_user" | cut -d: -f6)"
if ((EUID != 0)); then echo 'root is required to install systemd units' >&2; exit 2; fi
news_stage="$(mktemp -d /tmp/stockagent-news-units.XXXXXXXX)"
cleanup_news_stage() {
  for news_staged_unit in stockagent-news-fulltext.service stockagent-news-fulltext.timer; do
    if [[ -f "$news_stage/$news_staged_unit" && ! -L "$news_stage/$news_staged_unit" ]]; then
      unlink "$news_stage/$news_staged_unit"
    fi
  done
  rmdir "$news_stage" 2>/dev/null || true
}
trap cleanup_news_stage EXIT
for news_unit in stockagent-news-fulltext.service stockagent-news-fulltext.timer; do
  sed -e "s|@REPO_ROOT@|$news_repo_root|g" \
      -e "s|@SERVICE_USER@|$news_service_user|g" \
      -e "s|@SERVICE_GROUP@|$news_service_group|g" \
      -e "s|@SERVICE_HOME@|$news_service_home|g" \
      "$news_repo_root/deploy/systemd/$news_unit.in" > "$news_stage/$news_unit"
done
systemd-analyze verify "$news_stage/stockagent-news-fulltext.service" "$news_stage/stockagent-news-fulltext.timer"
for news_unit in stockagent-news-fulltext.service stockagent-news-fulltext.timer; do
  install -m 0644 "$news_stage/$news_unit" "/etc/systemd/system/$news_unit"
  unlink "$news_stage/$news_unit"
done
systemctl daemon-reload
systemctl enable --now stockagent-news-fulltext.timer
