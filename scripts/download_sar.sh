#!/usr/bin/env bash
# Script to build Docker image and run container for SAR data download pipeline
# This pipeline only downloads SAR ZIP files and lists them; processing/upload is handled separately.
# Enforce strict Bash settings
set -euo pipefail

# Resolve script directory (host path)
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Docker image name
IMAGE_NAME="download_sar_pipeline:latest"

usage() {
    cat <<EOF
Usage: $0 --start-date YYYY-MM-DD --end-date YYYY-MM-DD --boundary-file FILE [--credentials-file FILE] [--buffer-km KM] [--output-dir DIR] [--help]

Options:
  --start-date YYYY-MM-DD  Start date of search window (required).
  --end-date YYYY-MM-DD    End date of search window (required).
  --boundary-file FILE     Area boundary GeoJSON file (required).
  --credentials-file FILE  Credentials file basename (default: credentials).
  --buffer-km KM           Buffer distance in kilometers (default: 0).
  --output-dir DIR         Directory where ZIP files are saved (default: downloads/).
  --help                   Display this help and exit.
EOF
}

# Default option values
credentials_file="credentials"
boundary_file=""
buffer_km="0"
output_dir="${script_dir}/downloads"
start_date=""
end_date=""

SHORTOPTS=
LONGOPTS=start-date:,end-date:,credentials-file:,boundary-file:,buffer-km:,output-dir:,help

if ! PARSED_OPTS=$(getopt --options "$SHORTOPTS" --longoptions "$LONGOPTS" --name "$0" -- "$@"); then
    usage
    exit 2
fi
eval set -- "$PARSED_OPTS"
while true; do
    case "$1" in
        --start-date)
            start_date="$2"; shift 2;;
        --end-date)
            end_date="$2"; shift 2;;
        --credentials-file)
            credentials_file="$2"; shift 2;;
        --boundary-file)
            boundary_file="$2"; shift 2;;
        --buffer-km)
            buffer_km="$2"; shift 2;;
        --output-dir)
            output_dir="$2"; shift 2;;
        --help)
            usage; exit 0;;
        --)
            shift; break;;
        *)
            echo "Unexpected option: $1" >&2; usage; exit 3;;
    esac
done

if [[ -z "$start_date" || -z "$end_date" ]]; then
    echo "Error: --start-date and --end-date are required." >&2
    usage
    exit 4
fi

if [[ -z "$boundary_file" ]]; then
    echo "Error: --boundary-file is required." >&2
    usage
    exit 5
fi

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

output_dir="$(resolve_path_portable "${output_dir}")"
mkdir -p "${output_dir}"
echo "[INFO] Using area boundary GeoJSON file: ${boundary_file}"
echo "[INFO] Using buffer distance (km): ${buffer_km}"
echo "[INFO] Downloading data from ${start_date} to ${end_date}"
echo "[INFO] Saving downloads to: ${output_dir}"

echo "[INFO] Building Docker image ${IMAGE_NAME}..."
docker build -t "${IMAGE_NAME}" "${script_dir}"

echo "[INFO] Running Docker container for SAR data download pipeline..."
docker run --rm \
  -v "${script_dir}":/app \
  -v "${output_dir}":/app/downloads \
  -v "${script_dir}/../grids":/app/grids:ro \
  -w /app "${IMAGE_NAME}" "$credentials_file" "$boundary_file" "$buffer_km" "$start_date" "$end_date"
