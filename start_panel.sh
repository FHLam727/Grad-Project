#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
if [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "Could not find a usable Python interpreter. Install Python 3 first." >&2
  exit 1
fi

export MACAU_ANALYTICS_DB_PATH="${MACAU_ANALYTICS_DB_PATH:-$PROJECT_ROOT/macau_analytics.db}"
export PROJECT_ANALYTICS_DB_PATH="${PROJECT_ANALYTICS_DB_PATH:-$PROJECT_ROOT/data/social_media_analytics.db}"

"$PYTHON_BIN" - <<'PY'
import importlib
import sys

missing = []
for module_name, import_name in (
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
    ("pandas", "pandas"),
    ("python-dotenv", "dotenv"),
    ("jieba", "jieba"),
):
    try:
        importlib.import_module(import_name)
    except Exception:
        missing.append(module_name)

if missing:
    print(
        "Missing dependencies: " + ", ".join(missing) + "\n"
        "Install them with: python3 -m pip install -r requirements_extra.txt",
        file=sys.stderr,
    )
    sys.exit(1)
PY

cd "$PROJECT_ROOT"
exec "$PYTHON_BIN" -m uvicorn bridge:app --reload --host 127.0.0.1 --port 9038
