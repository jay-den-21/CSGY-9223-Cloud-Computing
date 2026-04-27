#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${1:-$SCRIPT_DIR/.env}"
OUT_FILE="$SCRIPT_DIR/config.js"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  echo "Copy .env.example to .env, fill in the values, then rerun this script." >&2
  exit 1
fi

python3 - "$ENV_FILE" "$OUT_FILE" <<'PY'
import json
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
required_keys = ("API_BASE_URL", "API_KEY")


def strip_quotes(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


config = {}

for raw_line in env_path.read_text().splitlines():
    line = raw_line.strip()

    if not line or line.startswith("#"):
        continue

    if line.startswith("export "):
        line = line[len("export "):].strip()

    if "=" not in line:
        continue

    key, value = line.split("=", 1)
    key = key.strip()

    if key in required_keys:
        config[key] = strip_quotes(value)

missing = [key for key in required_keys if not config.get(key)]

if missing:
    print(f"Missing required env values: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

out_path.write_text(
    "window.APP_CONFIG = Object.freeze("
    + json.dumps(config, indent=2)
    + ");\n"
)
PY

echo "Wrote $OUT_FILE"
