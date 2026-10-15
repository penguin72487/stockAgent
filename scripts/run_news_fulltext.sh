#!/usr/bin/env bash
set -euo pipefail
news_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$news_repo_root"
source "$news_repo_root/scripts/runtime_env.sh"
exec_fintech_python -m scripts.download_news_fulltext run --continuous \
  --config "$news_repo_root/configs/news_fulltext.json" \
  --max-articles "${NEWS_FULLTEXT_MAX_ARTICLES:-512}" \
  --max-discovery-requests "${NEWS_FULLTEXT_MAX_DISCOVERY_REQUESTS:-512}"
