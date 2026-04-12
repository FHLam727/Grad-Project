#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv_full_web"
HOST="${FULL_WEB_HOST:-127.0.0.1}"
PORT="${FULL_WEB_PORT:-9038}"
RELOAD="${FULL_WEB_RELOAD:-0}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Missing ${VENV_DIR}. Run ./bootstrap_full_web.sh first."
  exit 1
fi

if [[ -z "${DB_PATH:-}" ]]; then
  DEFAULT_MAIN_DB="${ROOT_DIR}/macau_analytics.db"
  if [[ -f "${DEFAULT_MAIN_DB}" ]]; then
    export DB_PATH="${DEFAULT_MAIN_DB}"
  fi
fi

if [[ -z "${FULL_WEB_ANALYTICS_DB_PATH:-}" ]]; then
  DEFAULT_DB="${ROOT_DIR}/data/social_media_analytics.db"
  if [[ -f "${DEFAULT_DB}" ]]; then
    export FULL_WEB_ANALYTICS_DB_PATH="${DEFAULT_DB}"
  else
    cat <<EOF
Full-Web database not configured.

Either:
1. Put the database at:
   ${DEFAULT_DB}
or
2. Export:
   FULL_WEB_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db
EOF
    exit 1
  fi
fi

echo "Starting Grad-Project with Full-Web entry on http://${HOST}:${PORT}"
echo "Using FULL_WEB_ANALYTICS_DB_PATH=${FULL_WEB_ANALYTICS_DB_PATH}"
if [[ -n "${DB_PATH:-}" ]]; then
  echo "Using DB_PATH=${DB_PATH}"
else
  echo "DB_PATH is not set. Main-system auth routes may fail without macau_analytics.db."
fi

if [[ "${RELOAD}" == "1" ]]; then
  echo "Starting with auto-reload enabled"
  "${VENV_DIR}/bin/python" -m uvicorn bridge:app --reload --host "${HOST}" --port "${PORT}"
else
  echo "Starting without auto-reload for a faster, more stable launch"
  "${VENV_DIR}/bin/python" -m uvicorn bridge:app --host "${HOST}" --port "${PORT}"
fi
