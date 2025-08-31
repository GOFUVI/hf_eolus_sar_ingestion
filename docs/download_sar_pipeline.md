# Sentinel-1 SAR Download Pipeline

## Overview
The `scripts/download_sar.sh` script orchestrates the retrieval of Sentinel-1 Ocean (OCN) SAR products.
It builds a Docker image containing the R-based downloader, launches a container to execute the
R script, and stores the resulting ZIP archives and metadata on the host machine.

## Requirements
- **Docker**: used to build and run the isolated download environment.
- **Internet access**: required to fetch container images, install R packages, and download SAR data.
- **Credentials file**: plain text file with the S1OCN username on the first line and the password on the second line.
- **Area boundary GeoJSON**: polygon defining the search area for SAR scenes.

## Components
### `scripts/download_sar.sh`
Bash wrapper that performs the following steps:
1. Parses command line options such as date range, credentials file, boundary file, buffer distance, and output directory.
2. Builds a Docker image using the `scripts/Dockerfile` file.
3. Runs a container, mounting the downloads directory and boundary data, and passes the collected arguments to the R entrypoint.

### `scripts/Dockerfile`
Defines the container image used by the pipeline. It:
- Derives from `rocker/geospatial`.
- Installs required R packages including `magrittr`, `purrr`, `dplyr`, `stringr`, `sf`, `arrow`, `here`, and the remote `S1OCN` package.
- Copies `download_SAR.R` into the image and sets it as the default entrypoint.

### `scripts/download_SAR.R`
R script executed inside the container. It:
1. Reads command line arguments for credentials, boundary file, buffer distance, and the start and end dates.
2. Uses `sf` to load and optionally buffer the area boundary, converting it to a bounding box for queries.
3. Authenticates against the S1OCN service and lists candidate Sentinel-1 files within the requested window.
4. Writes `files_to_download.csv` with the metadata of the files to be retrieved.
5. Downloads missing ZIP archives to the `downloads/` directory, removing obsolete files.
6. Outputs `downloaded_files.txt` listing the absolute paths of the ZIP archives.

## Usage
```bash
bash scripts/download_sar.sh \
  --start-date YYYY-MM-DD --end-date YYYY-MM-DD --boundary-file FILE \
  [--credentials-file FILE] [--buffer-km KM] [--output-dir DIR]
```

### Options
- `--start-date YYYY-MM-DD`: Beginning of the search window. **Required**.
- `--end-date YYYY-MM-DD`: End of the search window. **Required**.
- `--boundary-file FILE`: GeoJSON boundary file. **Required**.
- `--credentials-file FILE`: Path to the credentials file. Default: `credentials`.
- `--buffer-km KM`: Buffer to apply around the boundary in kilometers. Default: `0`.
- `--output-dir DIR`: Directory where the downloaded ZIP files are stored. Default: `scripts/downloads/`.
- `--help`: Display usage information.

### Path Resolution
- Relative paths for `--credentials-file` and `--boundary-file` are resolved with respect to the script directory (`scripts/`), not your current shell directory.
- By default, the script looks for a credentials file named `credentials` next to the script: `scripts/credentials`.
- For the boundary, the default is relative to the script directory: `scripts/../grids/.cache_grids/area_boundary.geojson`. If you place a boundary file next to the script (e.g., `scripts/area_boundary.geojson`), pass `--boundary-file area_boundary.geojson`.

### Boundary File Spec
- Format: Valid GeoJSON file (`.geojson`).
- Geometry: A single Polygon or MultiPolygon. You may also provide a Feature or FeatureCollection containing exactly one polygonal feature; any properties are ignored.
- CRS: EPSG:4326 (WGS84) with coordinates as `[longitude, latitude]` in decimal degrees. Do not supply projected coordinates; the script does not reproject.
- Validity: Geometry must be valid (no self-intersections) and rings must be closed. Orientation does not matter.
- Size: Keep the polygon reasonably simple; the pipeline computes a bounding box over the (optionally buffered) geometry for the search.
- Buffer: `--buffer-km` applies a geodesic buffer of the given kilometers before taking the bounding box.

Minimal example (single polygon FeatureCollection):
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "Polygon",
        "coordinates": [[
          [-3.70379, 40.41678],
          [-3.60379, 40.41678],
          [-3.60379, 40.51678],
          [-3.70379, 40.51678],
          [-3.70379, 40.41678]
        ]]
      }
    }
  ]
}
```

## Example
```bash
bash scripts/download_sar.sh \
  --start-date 2024-01-01 --end-date 2024-01-03 \
  --credentials-file ~/.config/s1ocn.txt \
  --boundary-file data/area_boundary.geojson \
  --buffer-km 5 \
  --output-dir /tmp/sar_downloads
```

The downloads directory will contain ZIP archives for each selected scene, alongside:
- `files_to_download.csv` – list of candidate files before downloading.
- `downloaded_files.txt` – absolute paths of the successfully retrieved ZIPs.

## Notes
This document focuses solely on the download pipeline. Subsequent ingestion scripts such as
`ingest_sar.sh` are out of scope.

Aviso: aunque el script intenta reanudar la descarga cuando detecta algún problema (por ejemplo, cortes de red o fallos temporales), en ocasiones pueden producirse errores desconocidos que no puede gestionar y el proceso puede detenerse. Por ello, no es recomendable ejecutarlo completamente desatendido. Si tras un fallo se vuelve a lanzar el script en el mismo directorio de trabajo/salida, retomará la descarga en el punto donde quedó sin repetir lo ya completado.
