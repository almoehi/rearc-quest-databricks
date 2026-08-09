#!/usr/bin/env bash
# Bootstrap Unity Catalog for the BLS pipeline.
# Handles: catalog creation, account groups, membership, catalog-level grants.
# Schema and volume grants are managed separately by 'databricks bundle deploy'.
#
# Requires: Databricks CLI (account-admin + metastore-admin credentials), jq
# Usage:   CATALOG=dev_bls ./pipeline/setup.sh
#          CATALOG=prod_bls DB_PROFILE=prod ./pipeline/setup.sh
set -euo pipefail

CATALOG="${CATALOG:-rearc_dev}"
DB_PROFILE="${DB_PROFILE:-dev}"

db() {
  # Wrapper so every databricks call uses the configured CLI profile
  databricks -p "$DB_PROFILE" "$@"
}

# ── helpers ───────────────────────────────────────────────────────────────────

get_or_create_group() {
  local name="$1"
  local id
  # -o json forces JSON output; filter client-side because --filter is unreliable
  id=$(db groups list -o json 2>/dev/null | jq -r --arg n "$name" '
    (if type == "object" then (.Resources // []) else . end)
    | .[] | select(.displayName == $n) | .id | tostring' | head -1)
  if [[ -n "$id" ]]; then
    echo "group '$name' already exists (id=$id)" >&2
  else
    id=$(db groups create -o json \
      --json "{\"displayName\":\"$name\"}" | jq -r '.id | tostring')
    echo "created group '$name' (id=$id)" >&2
  fi
  echo "$id"
}

add_member() {
  # PATCH add is a no-op when the user is already a member.
  # Use jq -n to build the payload so numeric/special-char IDs are safe.
  local payload
  payload=$(jq -n --arg uid "$2" '{
    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
    "Operations": [{"op":"add","path":"members","value":[{"value":$uid}]}]
  }')
  db groups patch "$1" --json "$payload"
}

# ── 1. Catalog ────────────────────────────────────────────────────────────────
# On Databricks free edition the metastore uses account-managed storage and new
# catalogs must be created via the UI; the CLI create call will fail with a
# "Metastore storage root URL does not exist" error in that case.

if db catalogs get "$CATALOG" &>/dev/null; then
  echo "catalog '$CATALOG' already exists"
else
  echo "creating catalog '$CATALOG'..."
  err=$(db catalogs create \
    --json "{\"name\":\"$CATALOG\",\"comment\":\"BLS productivity pipeline [${CATALOG}]\"}" 2>&1) \
  || {
    cat >&2 <<EOF

ERROR: could not create catalog '$CATALOG' via CLI:
  $err

On Databricks free edition, create the catalog manually:
  1. Open the Databricks UI → Catalog (left sidebar) → + Create catalog
  2. Set name to '$CATALOG', select 'Databricks-managed storage' (default)
  3. Click Create
  4. Re-run this script — the catalog step will be skipped automatically

EOF
    exit 1
  }
  echo "created catalog '$CATALOG'"
fi

# ── 2. Groups ─────────────────────────────────────────────────────────────────

ENG_ID=$(get_or_create_group "engineering")
BU_ID=$(get_or_create_group "business users")

# ── 3. User membership ────────────────────────────────────────────────────────

ME=$(db current-user me -o json 2>/dev/null)
USER_ID=$(echo "$ME" | jq -r '.id | tostring')
USER_NAME=$(echo "$ME" | jq -r '.userName // .displayName // "unknown"')

if [[ -z "$USER_ID" || "$USER_ID" == "null" ]]; then
  echo "ERROR: could not resolve current user" >&2
  exit 1
fi

echo "current user: $USER_NAME (id=$USER_ID)"

echo "adding almoehi@gmail.com (id=$USER_ID) to both groups"
add_member "$ENG_ID" "$USER_ID"
add_member "$BU_ID"  "$USER_ID"

# ── 4. Catalog-level grants ───────────────────────────────────────────────────
#
# UC grants require account-level groups or user emails as principals.
# Workspace-local groups (created above) are not visible to UC on the free
# edition, so we grant directly to the current user.

db grants update catalog "$CATALOG" --json "{
  \"changes\": [
    {\"principal\": \"$USER_NAME\", \"add\": [\"USE_CATALOG\", \"CREATE_SCHEMA\", \"MANAGE\"]}
  ]
}"

# ── 5. Schemas ────────────────────────────────────────────────────────────────
# DLT creates tables but NOT schemas. Schemas must exist before 'bundle deploy'
# can create the volume (bronze) and before the pipeline run populates gold/silver.

for SCHEMA in bronze silver gold; do
  if db schemas get "${CATALOG}.${SCHEMA}" &>/dev/null; then
    echo "schema '${CATALOG}.${SCHEMA}' already exists"
  else
    echo "creating schema '${CATALOG}.${SCHEMA}'..."
    db schemas create "$SCHEMA" "$CATALOG"
    echo "created schema '${CATALOG}.${SCHEMA}'"
  fi
done

# ── 6. Shared workspace folder for Genie Spaces ──────────────────────────────

GENIE_PATH="/Workspace/Shared/rearc_genie_spaces"
echo "ensuring workspace folder '$GENIE_PATH' exists..."
db workspace mkdirs "$GENIE_PATH"

echo ""
echo "catalog bootstrap complete for '${CATALOG}' (profile: ${DB_PROFILE})"
echo "next: run.sh  (deploy → run pipeline → full deploy)"
