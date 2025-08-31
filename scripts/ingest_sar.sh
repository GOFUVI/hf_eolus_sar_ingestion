#!/usr/bin/env bash
set -euo pipefail

# Timestamped logging helpers
ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log_info() { echo "[$(ts)] [INFO] $*"; }
log_warn() { echo "[$(ts)] [WARN] $*"; }
log_err()  { echo "[$(ts)] [ERROR] $*" 1>&2; }

# Script to build Docker image and run container for SAR data ingest pipeline
# This pipeline processes downloaded ZIPs into Parquet and ingests them to S3.

usage() {
    cat <<'EOF'
Usage: $0 --s3-uri S3_URI --profile PROFILE --athena-db DB --athena-table TABLE \
          --collection ID [--region REGION] [--build-opts OPTS] [--data-dir DIR] \
          [--output-dir DIR] [--stac-item-properties-json FILE] \
          [--stac-collection-properties-json FILE] [--verbose] [--keep-output] [--help]

Options:
  --s3-uri S3_URI      Target S3 URI for Parquet output and Athena resources.
  --profile PROFILE    AWS CLI profile to use.
  --athena-db DB       Athena database name.
  --athena-table TABLE Athena table name.
  --collection ID      STAC collection identifier.
  --region REGION      AWS region; if omitted, retrieved from profile.
  --build-opts OPTS    Additional options for docker build.
  --data-dir DIR       Directory containing downloaded ZIP files (default: downloads/).
  --output-dir DIR     Root directory for catalog output (default: catalog_output/).
  --stac-item-properties-json FILE       JSON file with extra STAC item properties.
  --stac-collection-properties-json FILE JSON file with extra STAC collection properties.
  --verbose            Enable verbose (set -x) mode.
  --keep-output        Do not remove existing output directory.
  --help               Display this help and exit.
EOF
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="ingest_sar_pipeline:latest"

# Default option values
region=""
build_opts=""
verbose=0
clean_output=1
s3_uri=""
profile=""
athena_db=""
athena_table=""
data_dir="${script_dir}/downloads"
output_dir="${script_dir}/catalog_output"
collection=""
stac_item_props=""
stac_collection_props=""

SHORTOPTS=
LONGOPTS=region:,build-opts:,data-dir:,output-dir:,collection:,stac-item-properties-json:,stac-collection-properties-json:,verbose,keep-output,s3-uri:,profile:,athena-db:,athena-table:,help

if ! PARSED_OPTS=$(getopt --options "$SHORTOPTS" --longoptions "$LONGOPTS" --name "$0" -- "$@"); then
  usage
  exit 2
fi
eval set -- "$PARSED_OPTS"
while true; do
  case "$1" in
    --region)
      region="$2"; shift 2;;
    --build-opts)
      build_opts="$2"; shift 2;;
    --data-dir)
      data_dir="$2"; shift 2;;
    --output-dir)
      output_dir="$2"; shift 2;;
    --verbose)
      verbose=1; shift;;
    --keep-output)
      clean_output=0; shift;;
    --s3-uri)
      s3_uri="$2"; shift 2;;
    --profile)
      profile="$2"; shift 2;;
    --athena-db)
      athena_db="$2"; shift 2;;
    --athena-table)
      athena_table="$2"; shift 2;;
    --collection)
      collection="$2"; shift 2;;
    --stac-item-properties-json)
      stac_item_props="$2"; shift 2;;
    --stac-collection-properties-json)
      stac_collection_props="$2"; shift 2;;
    --help)
      usage; exit 0;;
    --)
      shift; break;;
    *)
      echo "[ERROR] Unexpected option: $1" >&2; usage; exit 3;;
  esac

done

if [[ -z "$s3_uri" || -z "$profile" || -z "$athena_db" || -z "$athena_table" || -z "$collection" ]]; then
  echo "[ERROR] --s3-uri, --profile, --athena-db, --athena-table, and --collection are required" >&2
  usage
  exit 1
fi

if [[ $verbose -eq 1 ]]; then
  set -x
  log_info "Verbose mode enabled (bash xtrace)"
fi

log_info "Parameters: s3_uri='${s3_uri}', profile='${profile}', athena_db='${athena_db}', athena_table='${athena_table}', collection='${collection}'"

# Portable path resolver (macOS/BSD-friendly, allows non-existing paths)
resolve_path_portable() {
  local in_path="$1"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$in_path" <<'PY'
import os, sys
p = sys.argv[1]
p = os.path.expandvars(os.path.expanduser(p))
print(os.path.abspath(p))
PY
  else
    case "$in_path" in
      /*) printf '%s\n' "$in_path" ;;
      ~*) printf '%s\n' "${in_path/#\~/$HOME}" ;;
      *)  printf '%s\n' "$(pwd)/$in_path" ;;
    esac
  fi
}

# Resolve data directory path and validate
data_dir="$(resolve_path_portable "${data_dir}")"
if [[ ! -d "${data_dir}" ]]; then
  log_err "Data directory '${data_dir}' does not exist"
  exit 1
fi
log_info "Using data directory '${data_dir}'"

# Resolve output directory path
output_dir="$(resolve_path_portable "${output_dir}")"
log_info "Using output directory '${output_dir}'"

# Determine AWS region if not specified
if [[ -z "$region" ]]; then
  region="$(aws configure get region --profile "${profile}" 2>/dev/null || true)"
  if [[ -n "$region" ]]; then
    log_info "Using AWS region '${region}' from profile '${profile}'."
  else
    log_warn "AWS region not specified and could not be retrieved from profile '${profile}'."
  fi
else
  log_info "Using AWS region '${region}'."
fi

log_info "Building Docker image ${IMAGE_NAME}${build_opts:+ (options: $build_opts)}..."
# shellcheck disable=SC2086
docker build ${build_opts} -f "${script_dir}/Dockerfile.upload" -t "${IMAGE_NAME}" "${script_dir}"

# Run upload_SAR.R in the container to produce Parquet dataset and Athena schema
log_info "Running Docker container for SAR data ingest pipeline..."

# Prepare run log
LOG_DIR="${script_dir}/logs"
mkdir -p "${LOG_DIR}"
RUN_LOG="${LOG_DIR}/ingest_sar_$(date +%Y%m%d_%H%M%S).log"
log_info "Teeing container output to ${RUN_LOG}"

# Clean previous output unless --keep-output is set
if [[ $clean_output -eq 1 ]]; then
  if [[ -d "${output_dir}" ]]; then
    log_info "Cleaning existing output directory: ${output_dir}"
    rm -rf "${output_dir}"
  else
    log_info "No existing output directory to clean"
  fi
fi

mkdir -p "${output_dir}"
assets_dir="${output_dir}/assets"
mkdir -p "${assets_dir}"

docker run --rm \
  -v "${script_dir}":/app \
  -v "${data_dir}":/app/downloads \
  -v "${assets_dir}":/app/parquet_output \
  -w /app "${IMAGE_NAME}" 2>&1 | tee "${RUN_LOG}"

# Quick sanity check of output presence
if [[ -d "${assets_dir}" ]]; then
  FILE_COUNT=$(find "${assets_dir}" -type f -name "*.parquet" | wc -l | awk '{print $1}')
  log_info "assets directory exists. Parquet files found: ${FILE_COUNT}"
  if [[ "${FILE_COUNT}" -eq 0 ]]; then
    log_warn "No Parquet files generated under '${assets_dir}'"
  fi
  # Debug aid: confirm lineage.json presence after container run
  if [[ -f "${assets_dir}/lineage.json" ]]; then
    log_info "Found lineage mapping at ${assets_dir}/lineage.json"
  else
    log_warn "Lineage mapping not present at ${assets_dir}/lineage.json immediately after container run"
    ls -la "${assets_dir}" || true
  fi
else
  log_err "Output directory '${assets_dir}' not found after container run"
fi

# Build STAC catalog before uploading
STAC_IMAGE="sar_stac_catalog:latest"
log_info "Building STAC catalog image ${STAC_IMAGE}..."
# shellcheck disable=SC2086
docker build ${build_opts} -f "${script_dir}/Dockerfile.catalog" -t "${STAC_IMAGE}" "${script_dir}" >/dev/null
log_info "Running STAC catalog builder..."
docker_args=(-v "${output_dir}":/data)
# Ensure optional arg arrays are defined even if unused (safe under set -u)
declare -a item_arg=()
declare -a coll_arg=()
if [[ -n "${stac_item_props}" ]]; then
  resolved_item="$(resolve_path_portable "${stac_item_props}")"
  docker_args+=(-v "${resolved_item}":/tmp/item_props.json:ro)
  item_arg=(--item-properties /tmp/item_props.json)
fi
if [[ -n "${stac_collection_props}" ]]; then
  resolved_coll="$(resolve_path_portable "${stac_collection_props}")"
  docker_args+=(-v "${resolved_coll}":/tmp/collection_props.json:ro)
  coll_arg=(--collection-properties /tmp/collection_props.json)
fi
# Use ${var+...} to avoid unbound errors when arrays are empty/unset
docker run --rm \
  "${docker_args[@]}" \
  "${STAC_IMAGE}" /data \
  --collection-id "${collection}" \
  ${item_arg+"${item_arg[@]}"} \
  ${coll_arg+"${coll_arg[@]}"}

# Annotate STAC items with processing lineage
log_info "Adding processing extension lineage to STAC items"
lineage_file="${assets_dir}/lineage.json"
if [[ ! -f "${lineage_file}" ]]; then
  log_warn "Lineage mapping file ${lineage_file} not found"
fi
for item_file in "${output_dir}/items"/*.json; do
  item_id="$(basename "$item_file" .json)"
  lineage_ids=""
  if [[ -f "${lineage_file}" ]]; then
    lineage_ids=$(jq -r --arg id "${item_id}" '.[$id] | join(", ")' "${lineage_file}")
  fi
  if [[ -n "${lineage_ids}" && "${lineage_ids}" != "null" ]]; then
    lineage_text="Derived from Sentinel-1 Level-2 OCN OWI products ${lineage_ids} using ingest_sar.sh (https://doi.org/10.5281/zenodo.17011788)"
  else
    log_warn "No lineage info for item ${item_id}"
    lineage_text="Derived using ingest_sar.sh (https://doi.org/10.5281/zenodo.17011788)"
  fi
  jq --arg lineage "$lineage_text" \
    '.stac_extensions |= (. + ["https://stac-extensions.github.io/processing/v1.1.0/schema.json"] | unique) |
     .properties["processing:lineage"] = $lineage' \
    "${item_file}" > "${item_file}.tmp" && mv "${item_file}.tmp" "${item_file}"
done
if [[ -f "${lineage_file}" ]]; then
  rm -f "${lineage_file}"
  log_info "Removed temporary lineage file ${lineage_file}"
else
  log_warn "Lineage mapping file ${lineage_file} was not found for cleanup"
fi

###############################################
# Prepare Athena schema and clean temp artifact
###############################################
# Read Athena column definitions generated by upload_SAR.R
schema_file_path="${output_dir}/assets/columns.sql"
if [[ -f "${schema_file_path}" ]]; then
  # Read base columns
  columns_sql=$(< "${schema_file_path}")
  # Remove partition column (date)
  columns_sql=$(echo "${columns_sql}" | sed -E 's/(^|, *)date [^,]+//g; s/^, *//; s/, *$//')
  # Format for pretty-printing in DDL
  # shellcheck disable=SC2001
  formatted_cols=$(echo "${columns_sql}" | sed 's/, */,\n  /g')
  # Remove temporary schema file from assets before uploading catalog
  rm -f "${schema_file_path}"
  log_info "Removed temporary schema file ${schema_file_path}"
else
  log_warn "Schema file not found at ${schema_file_path}; proceeding without removal"
fi

# Upload catalog to S3 after removing temporary files
log_info "Uploading catalog to ${s3_uri}"
upload_cmd=(aws s3 cp --recursive "${output_dir}" "${s3_uri}" --profile "${profile}")
if [[ -n "${region}" ]]; then
  upload_cmd+=(--region "${region}")
fi
"${upload_cmd[@]}"
log_info "Creating Athena table \"${athena_db}.${athena_table}\" with exported schema"
create_table_query=$(cat <<EOF
CREATE EXTERNAL TABLE IF NOT EXISTS ${athena_db}.${athena_table} (
  ${formatted_cols}
)
PARTITIONED BY (
  date STRING
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION '${s3_uri%/}/assets';
EOF
)
log_info "Athena CREATE EXTERNAL TABLE DDL:"
ddl_file="${script_dir}/athena_create_table.sql"
printf '%s\n' "${create_table_query}" > "${ddl_file}"
log_info "Athena CREATE EXTERNAL TABLE DDL saved to ${ddl_file}"

# Ensure Athena database exists before table operations
log_info "Ensuring Athena database ${athena_db} exists"
create_db_query="CREATE DATABASE IF NOT EXISTS ${athena_db};"
create_db_file="${script_dir}/athena_create_db.sql"
printf '%s\n' "${create_db_query}" > "${create_db_file}"
log_info "Athena CREATE DATABASE DDL saved to ${create_db_file}"
create_db_qid=$(aws athena start-query-execution \
  --query-string "file://${create_db_file}" \
  --result-configuration OutputLocation="${s3_uri}/athena_query_results/" \
  --region "${region}" --profile "${profile}" \
  --output text --query 'QueryExecutionId')
log_info "Athena CREATE DATABASE query started with execution ID: ${create_db_qid}"
status="RUNNING"
while [[ "$status" == "RUNNING" ]]; do
  sleep 5
  status=$(aws athena get-query-execution \
    --query-execution-id "$create_db_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.State')
  log_info "Athena CREATE DATABASE state: $status"
done
if [[ "$status" != "SUCCEEDED" ]]; then
  reason=$(aws athena get-query-execution \
    --query-execution-id "$create_db_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.StateChangeReason' || true)
  log_err "Athena CREATE DATABASE failed: $status${reason:+ - $reason}"
  exit 1
fi

# Drop existing table to ensure changes apply
log_info "Dropping existing Athena table ${athena_db}.${athena_table} if exists"
drop_query="DROP TABLE IF EXISTS ${athena_db}.${athena_table};"
drop_file="${script_dir}/athena_drop_table.sql"
printf '%s\n' "${drop_query}" > "${drop_file}"
log_info "Athena DROP TABLE DDL saved to ${drop_file}"
drop_qid=$(aws athena start-query-execution \
  --query-string "file://${drop_file}" \
  --query-execution-context Database="${athena_db}" \
  --result-configuration OutputLocation="${s3_uri}/athena_query_results/" \
  --region "${region}" --profile "${profile}" \
  --output text --query 'QueryExecutionId')
log_info "Athena DROP TABLE query started with execution ID: ${drop_qid}"
# Poll DROP TABLE query until completion
status="RUNNING"
while [[ "$status" == "RUNNING" ]]; do
  sleep 5
  status=$(aws athena get-query-execution \
    --query-execution-id "$drop_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.State')
  log_info "Athena DROP TABLE state: $status"
done
if [[ "$status" != "SUCCEEDED" ]]; then
  log_err "Athena DROP TABLE failed: $status"
  exit 1
fi

create_qid=$(aws athena start-query-execution \
  --query-string "file://${ddl_file}" \
  --query-execution-context Database="${athena_db}" \
  --result-configuration OutputLocation="${s3_uri}/athena_query_results/" \
  --region "${region}" --profile "${profile}" \
  --output text --query 'QueryExecutionId')
log_info "Athena CREATE TABLE query started with execution ID: ${create_qid}"

# Poll CREATE TABLE query status until completion
status="RUNNING"
while [[ "$status" == "RUNNING" ]]; do
  sleep 10
  status=$(aws athena get-query-execution \
    --query-execution-id "$create_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.State')
  log_info "Athena CREATE TABLE state: $status"
done
if [[ "$status" != "SUCCEEDED" ]]; then
  reason=$(aws athena get-query-execution \
    --query-execution-id "$create_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.StateChangeReason' || true)
  log_err "Athena CREATE TABLE failed: $status${reason:+ - $reason}"
  exit 1
fi

log_info "Repairing Athena table partitions"
repair_query="MSCK REPAIR TABLE ${athena_db}.${athena_table};"
repair_file="${script_dir}/athena_repair_table.sql"
printf '%s\n' "${repair_query}" > "${repair_file}"
log_info "Athena MSCK REPAIR DDL saved to ${repair_file}"
repair_qid=$(aws athena start-query-execution \
  --query-string "file://${repair_file}" \
  --query-execution-context Database="${athena_db}" \
  --result-configuration OutputLocation="${s3_uri}/athena_query_results/" \
  --region "${region}" --profile "${profile}" \
  --output text --query 'QueryExecutionId')
log_info "Athena MSCK REPAIR TABLE query started with execution ID: ${repair_qid}"

# Poll MSCK REPAIR TABLE query status until completion
status="RUNNING"
while [[ "$status" == "RUNNING" ]]; do
  sleep 10
  status=$(aws athena get-query-execution \
    --query-execution-id "$repair_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.State')
  log_info "Athena MSCK REPAIR TABLE state: $status"
done
if [[ "$status" != "SUCCEEDED" ]]; then
  reason=$(aws athena get-query-execution \
    --query-execution-id "$repair_qid" \
    --region "$region" --profile "$profile" \
    --output text --query 'QueryExecution.Status.StateChangeReason' || true)
  log_err "Athena MSCK REPAIR TABLE failed: $status${reason:+ - $reason}"
  exit 1
fi
