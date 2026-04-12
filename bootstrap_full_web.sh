#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv_full_web"

pick_python_bin() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    echo "${PYTHON_BIN}"
    return
  fi

  # Python 3.13 has been flaky for this project on macOS when bootstrapping the
  # Full-Web environment, so prefer 3.11/3.12 when available.
  for candidate in python3.11 python3.12 python3; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      echo "${candidate}"
      return
    fi
  done

  echo "python3"
}

PYTHON_BIN="$(pick_python_bin)"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python not found: ${PYTHON_BIN}"
  exit 1
fi

echo "Creating Full-Web virtual environment at ${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"

echo "Upgrading pip"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip

echo "Installing dependencies from requirements_extra.txt"
"${VENV_DIR}/bin/python" -m pip install -r "${ROOT_DIR}/requirements_extra.txt"

cat <<EOF

Bootstrap complete.

Next:
1. Put your Full-Web database at:
   ${ROOT_DIR}/data/social_media_analytics.db
   or export FULL_WEB_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db
2. Start the app with:
   ./run_full_web_sidecar.sh

Notes:
- Bootstrap used ${PYTHON_BIN}
- If you want live-reload while developing, run:
  FULL_WEB_RELOAD=1 ./run_full_web_sidecar.sh

EOF
