#!/usr/bin/env bash
# End-to-end pipeline run: bootstrap → deploy → ingest → full deploy.
#
# Idempotent: safe to re-run; existing resources are left in place.
#
# Usage:
#   ./pipeline/run.sh                      # uses profile=dev, target=dev
#   DB_PROFILE=prod TARGET=prod ./pipeline/run.sh
set -euo pipefail

DB_PROFILE="${DB_PROFILE:-dev}"
TARGET="${TARGET:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== [1/5] catalog + schema bootstrap ==="
CATALOG="${CATALOG:-rearc_dev}" DB_PROFILE="$DB_PROFILE" "$(dirname "$0")/setup.sh"

echo ""
echo "=== [2/5] deploy pipeline + volume (skip Genie Space — tables not yet created) ==="
databricks -p "$DB_PROFILE" bundle deploy -t "$TARGET" \
  --select bls_pipeline \
  --select bls_raw \
  --select bls_crawler

echo ""
echo "=== [3/5] run crawler (write raw files to volume) ==="
databricks -p "$DB_PROFILE" bundle run bls_crawler -t "$TARGET"

echo ""
echo "=== [4/5] run DLT pipeline (bronze → silver → gold) ==="
databricks -p "$DB_PROFILE" bundle run bls_pipeline -t "$TARGET"

echo ""
echo "=== [5/5] full deploy (Genie Space can now validate tables) ==="
databricks -p "$DB_PROFILE" bundle deploy -t "$TARGET"

echo ""
echo "Done. Pipeline is live at target '${TARGET}'."

GENIE_URL=$(SCRIPT_DIR="$SCRIPT_DIR" TARGET="$TARGET" DB_PROFILE="$DB_PROFILE" python3 - <<'EOF'
import configparser, json, os

cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser('~/.databrickscfg'))
host = cfg.get(os.environ['DB_PROFILE'], 'host', fallback='').rstrip('/')

state = f"{os.environ['SCRIPT_DIR']}/.databricks/bundle/{os.environ['TARGET']}/resources.json"
with open(state) as f:
    for k, v in json.load(f).get('state', {}).items():
        if 'genie_spaces' in k and 'permissions' not in k and '__id__' in v:
            print(f"{host}/genie/rooms/{v['__id__']}")
            break
EOF
)
echo "  Genie Space → ${GENIE_URL}"
