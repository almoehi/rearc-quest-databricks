#!/usr/bin/env bash
# Destroy all resources created by setup.sh and 'databricks bundle deploy'.
# Fully accepts data loss — drops catalog (and everything inside) with --force.
#
# Resources removed:
#   bundle-managed: DLT pipeline, crawler job, Genie Space, volume,
#                   bronze/silver/gold schemas (staging/prod targets)
#   setup.sh-managed: catalog, groups (engineering, business users),
#                     /Workspace/Shared/rearc_genie_spaces folder
#   DLT runtime-created: bronze/silver/gold schemas in dev (not bundle-managed)
#
# Usage:  CATALOG=rearc_dev ./pipeline/teardown.sh
#         CATALOG=prod_bls DB_PROFILE=prod BUNDLE_TARGET=prod ./pipeline/teardown.sh
set -euo pipefail

CATALOG="${CATALOG:-rearc_dev}"
DB_PROFILE="${DB_PROFILE:-dev}"
BUNDLE_TARGET="${BUNDLE_TARGET:-dev}"

db() {
  databricks -p "$DB_PROFILE" "$@"
}

echo "=================================================="
echo " TEARDOWN: catalog=$CATALOG  profile=$DB_PROFILE  target=$BUNDLE_TARGET"
echo " DATA LOSS IS PERMANENT — press Ctrl-C within 5s to abort"
echo "=================================================="
sleep 5

# ── 1. Bundle destroy ─────────────────────────────────────────────────────────
# Removes: DLT pipeline, crawler job, Genie Space, volume.
# Also removes bronze/silver/gold schemas for staging/prod targets (defined in
# databricks.yml per-target). Dev schemas are DLT runtime-created (step 2).

echo ""
echo "==> [1/5] destroying bundle resources (target: $BUNDLE_TARGET)..."
(
  cd "$(dirname "$0")"
  databricks -p "$DB_PROFILE" bundle destroy --target "$BUNDLE_TARGET" --auto-approve
) || echo "WARNING: bundle destroy failed or had nothing to destroy — continuing"

# ── 2. Drop schemas (CASCADE) ─────────────────────────────────────────────────
# In dev mode DLT creates bronze/silver/gold directly at runtime; they are not
# bundle-managed so bundle destroy won't touch them. Drop them explicitly.
# --force cascades to all tables, views, and volumes inside the schema.

echo ""
echo "==> [2/5] dropping schemas in catalog '$CATALOG'..."
for schema in bronze silver gold; do
  full="${CATALOG}.${schema}"
  if db schemas get "$full" &>/dev/null; then
    echo "    dropping schema $full..."
    db schemas delete "$full" --force \
      || echo "    WARNING: CLI delete failed for $full — you may need to run: DROP SCHEMA IF EXISTS \`${full}\` CASCADE"
  else
    echo "    schema $full not found, skipping"
  fi
done

# ── 3. Drop catalog (CASCADE) ─────────────────────────────────────────────────
# --force drops all remaining child objects (any schemas not caught above).

echo ""
echo "==> [3/5] dropping catalog '$CATALOG'..."
if db catalogs get "$CATALOG" &>/dev/null; then
  db catalogs delete "$CATALOG" --force \
    || echo "WARNING: CLI delete failed — you may need to run: DROP CATALOG IF EXISTS \`${CATALOG}\` CASCADE"
else
  echo "    catalog '$CATALOG' not found, skipping"
fi

# ── 4. Delete workspace folder ────────────────────────────────────────────────

GENIE_PATH="/Workspace/Shared/rearc_genie_spaces"
echo ""
echo "==> [4/5] deleting workspace folder '$GENIE_PATH'..."
db workspace delete "$GENIE_PATH" --recursive \
  || echo "    WARNING: could not delete '$GENIE_PATH' — may not exist or already gone"

# ── 5. Delete account groups ──────────────────────────────────────────────────

delete_group() {
  local name="$1"
  local id
  id=$(db groups list -o json 2>/dev/null | jq -r --arg n "$name" '
    (if type == "object" then (.Resources // []) else . end)
    | .[] | select(.displayName == $n) | .id | tostring' | head -1)
  if [[ -n "$id" && "$id" != "null" ]]; then
    echo "    deleting group '$name' (id=$id)..."
    db groups delete "$id" \
      || echo "    WARNING: could not delete group '$name' (id=$id)"
  else
    echo "    group '$name' not found, skipping"
  fi
}

echo ""
echo "==> [5/5] deleting groups..."
delete_group "engineering"
delete_group "business users"

echo ""
echo "=================================================="
echo " teardown complete for catalog '${CATALOG}' (profile: ${DB_PROFILE})"
echo "=================================================="
