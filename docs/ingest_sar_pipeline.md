# Sentinel-1 SAR Ingestion Pipeline

## Overview
The `scripts/ingest_sar.sh` script converts previously downloaded Sentinel-1 OCN ZIP archives into a GeoParquet dataset, builds a STAC catalog, uploads the results to an S3 bucket, and prepares an Athena table for querying. It orchestrates Docker builds, runs the R-based converter, generates STAC metadata, and performs S3/Athena operations.

## Requirements
- **Docker**: builds and runs the R and Python containers used in the pipeline.
- **AWS CLI**: required for uploading data to S3 and managing Athena resources. The script expects an AWS profile.
- **Existing SAR ZIP files**: output from the download step, stored under the data directory.
- **Optional STAC property JSON**: extra item or collection properties can be injected via JSON files.

## Components
### `scripts/ingest_sar.sh`
Bash orchestrator that:
1. Parses command-line options for S3 target, AWS profile, Athena details, collection ID, and paths.
2. Builds a Docker image using `scripts/Dockerfile.upload` and runs `upload_SAR.R` to create Parquet assets.
3. Builds a second image from `scripts/Dockerfile.catalog` and runs `build_sar_catalog.py` to generate STAC items and collection.
4. Annotates STAC items with processing lineage, uploads the catalog to S3, and issues Athena DDL statements to create and repair the table.

### `scripts/Dockerfile.upload`
Dockerfile that derives from `rocker/geospatial`, installs Python dependencies and AWS CLI in a virtual environment, installs required R packages (including `S1OCN`), copies `upload_SAR.R`, and sets it as the container entrypoint.

### `scripts/upload_SAR.R`
R script executed inside the upload container. It reads ZIP files from `downloads/`, extracts wind data using the `S1OCN` package, adds a date partition column, writes a partitioned GeoParquet dataset under `parquet_output/`, and exports a `columns.sql` file describing Athena column types.

### `scripts/Dockerfile.catalog`
Lightweight Python image that installs `pyarrow`, `shapely`, and `pystac[validation]`. It copies `build_sar_catalog.py` and `parquet_stac_utils.py` and uses the former as the entrypoint.

### `scripts/build_sar_catalog.py`
Python utility that scans Parquet files under `assets/`, creates one STAC item per file with the Table Extension, assembles them into a collection, normalizes HREFs, validates the catalog, and saves everything under `items/` and `collection.json`.

### `scripts/parquet_stac_utils.py`
Helper module providing:
- `PyarrowS3IO`, a PySTAC I/O adapter backed by `pyarrow` for reading and writing JSON in S3.
- `write_dataset_with_retry`, which writes Parquet datasets with exponential backoff on `SLOW_DOWN` errors.

## Usage
```bash
bash scripts/ingest_sar.sh \
  --s3-uri s3://bucket/prefix --profile my-profile \
  --athena-db sar_db --athena-table sar_owi \
  --collection s1-owi [--region us-east-1] \
  [--data-dir DIR] [--output-dir DIR] \
  [--stac-item-properties-json FILE] \
  [--stac-collection-properties-json FILE]
```

## Options
- `--s3-uri S3_URI`: Target S3 URI for Parquet assets and Athena files. **Required**.
- `--profile PROFILE`: AWS CLI profile. **Required**.
- `--athena-db DB`: Athena database name. **Required**.
- `--athena-table TABLE`: Athena table name. **Required**.
- `--collection ID`: STAC collection identifier. **Required**.
- `--region REGION`: AWS region; defaults to the profile's region.
- `--build-opts OPTS`: Extra options for `docker build`.
- `--data-dir DIR`: Directory with downloaded ZIPs. Default: `scripts/downloads/`.
- `--output-dir DIR`: Destination for Parquet and STAC catalog. Default: `scripts/catalog_output/`.
- `--stac-item-properties-json FILE`: JSON file with additional item properties.
- `--stac-collection-properties-json FILE`: JSON file with additional collection properties.
- `--verbose`: Enable bash tracing.
- `--keep-output`: Retain existing output directory instead of cleaning it.
- `--help`: Show usage information.

## Outputs
After successful execution the output directory contains:
- `assets/`: GeoParquet files partitioned by `date` (`YYYY-MM-DD`).
- `items/`: STAC item JSON files linking to each Parquet asset.
- `collection.json`: STAC collection metadata.
The script also creates Athena DDL files under `scripts/` and logs in `scripts/logs/`.

## Example
```bash
bash scripts/ingest_sar.sh \
  --s3-uri s3://my-bucket/sar_ingest \
  --profile default \
  --athena-db sar_db --athena-table sar_owi \
  --collection s1-owi \
  --data-dir /data/sar_zips \
  --output-dir /tmp/catalog_output
```

The Parquet dataset is uploaded to the specified S3 location, a STAC catalog is generated, and an Athena table is created and repaired for querying.
