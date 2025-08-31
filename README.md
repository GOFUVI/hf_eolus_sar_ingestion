# HF-EOLUS Sentinel-1 SAR Ingestion Pipeline

## Overview

This repository provides an end-to-end pipeline to acquire **Sentinel-1 SAR** Ocean (OCN) data and convert it into a cloud-optimized, analysis-ready format. It comprises two main Bash scripts -- `download_sar.sh` and `ingest_sar.sh` -- which serve as the user-facing interface (all other files are supporting components). Using these scripts, you can **download Sentinel-1 OCN products for a given time range and area**, then **ingest the data into a partitioned GeoParquet dataset with accompanying STAC metadata**, optionally uploading to cloud storage for immediate querying[\[1\]][][\[2\]].

-   **Download Stage (**`download_sar.sh`**):** Retrieves Sentinel-1 OCN Level-2 **Ocean Wind Fields (OWI)** ZIP archives for a specified date range and geographic region[\[2\]]. The script builds a Docker container containing an R-based downloader (utilizing the `S1OCN` R package) and runs it to authenticate to the Sentinel-1 OCN service, search for available scenes, and download the relevant ZIP files to your local storage[\[3\]][][\[4\]]. This stage produces a set of raw data files (OCN ZIPs) along with CSV/TXT logs of what was downloaded.

-   **Ingestion Stage (**`ingest_sar.sh`**):** Converts the downloaded SAR OCN archives into a partitioned **GeoParquet** dataset, generates a corresponding **STAC** (SpatioTemporal Asset Catalog) for metadata, and prepares the data for cloud-based analytics[\[1\]]. This script orchestrates multiple steps: it builds and runs a Docker image with an R environment to extract ocean wind data from each Sentinel-1 file and write it as Parquet (using the OGC GeoParquet format), then builds a second Docker image with Python tools to create STAC **Items** and a **Collection** JSON that describe the Parquet files (using the STAC Table Extension for tabular data)[\[5\]]. Finally, it uploads the Parquet files and STAC catalog to an Amazon S3 bucket and uses **AWS Athena** to create an external table for SQL querying of the data[\[1\]][][\[6\]]. After ingestion, scientists can query the SAR-derived dataset (e.g. wind measurements) using Athena or other data lake frameworks, and discover data via the STAC catalog.

**Under the hood:** The pipeline leverages containerized environments for reproducibility. The download stage uses a **Docker** image (based on `rocker/geospatial` R) that includes the `S1OCN` package and geospatial libraries to fetch data[\[7\]]. The ingestion stage uses two Docker images: an R-based image (to perform data extraction and Parquet conversion with the `S1OCN` and `arrow` packages)[\[8\]], and a lightweight Python image (with **PySTAC**, **pyarrow**, **shapely**, etc.) to build the STAC catalog[\[5\]]. These containers are built automatically by the scripts, so the user does not need to manually set up R or Python environments. The result of running the pipeline is a **GeoParquet** dataset (with embedded geospatial metadata) and a **STAC static catalog** describing that dataset, ready for integration into scientific workflows.

**Why GeoParquet & STAC?** Using **GeoParquet** ensures that each Parquet file produced includes standard geospatial metadata (geometry columns, Coordinate Reference System, etc.) in compliance with the **OGC GeoParquet v1.1.0** specification[\[9\]]. This yields efficient, compressed columnar files that are self-describing and can be read directly by tools like Pandas/GeoPandas, R, or SQL engines (e.g. Athena, DuckDB) without custom parsing. **STAC** metadata provides a standardized, machine-readable catalog for the dataset[\[10\]]. By publishing the Parquet assets with STAC, researchers can easily discover and access the data via common STAC tooling (such as STAC browsers or PySTAC)[\[11\]][][\[12\]]. The STAC Items include the **Table Extension** to expose the schema of the Parquet files (columns, data types) and are annotated with the **Processing Extension** to record lineage (each item\'s metadata notes which Sentinel-1 scenes contributed to that data)[\[13\]]. This approach ensures **interoperability** and **traceability**: data are stored in an analysis-ready form and described with community standards, aligning with the HF-EOLUS project\'s goal of efficient, standardized data management[\[14\]].

## Requirements

Before using the pipeline, ensure you have the following prerequisites:

-   **Operating System:** A Unix-like environment (the Bash scripts use GNU utilities). Linux is recommended; macOS should work with Docker installed (ensure GNU `getopt` is available), and Windows users can run via WSL or a similar POSIX environment.

-   **Docker** -- required to build and run the containerized R and Python environments[\[15\]][][\[16\]]. Install Docker and ensure the daemon is running. The first execution of each stage will download base images (e.g. rocker/geospatial) and install dependencies, which can take some time.

-   **AWS CLI** -- required for the ingestion stage to upload data to Amazon S3 and configure Athena. Install the AWS Command Line Interface and authenticate it with your AWS credentials. You should have an AWS account with S3 and Athena access, and configure an AWS CLI *profile* with the necessary keys/permissions[\[15\]]. The `ingest_sar.sh` script expects a profile name via `--profile`.

-   **AWS S3 Bucket & Athena** -- for cloud deployment of the results. You should have an S3 bucket (and optional prefix) ready to receive the Parquet files and STAC catalog. Also decide on an Athena **database** and **table** name where the external table will be created. (The Athena database can be created on the fly by the script if it doesn't exist.)

-   **Internet Access** -- needed to download container images on first run, to install R/Python packages inside Docker, and of course to download the Sentinel-1 data from the remote API[\[17\]].

-   **Sentinel-1 OCN Credentials** -- You need a valid username and password to access the Sentinel-1 OCN (Ocean) data service. Obtain these credentials from the data provider (e.g., via ESA/Copernicus Open Access if applicable) and store them in a text file (first line username, second line password)[\[18\]]. By default the download script looks for a file named `credentials` in the `scripts/` folder, but you can point to a custom file with `--credentials-file`.

-   **Area of Interest File** -- A GeoJSON file defining the geographic area of interest for downloading data[\[19\]]. This should contain a single Polygon or MultiPolygon (in WGS84 coordinates) delineating the region you want SAR data for. The script will use this polygon (optionally buffered) to find Sentinel-1 scenes overlapping that area. Ensure the GeoJSON geometry is valid (no self-intersections, etc.) and uses longitude/latitude coordinates in decimal degrees[\[20\]].

-   **Sufficient Storage** -- The raw Sentinel-1 OCN ZIP files and the resulting Parquet files can be quite large. Make sure you have enough disk space in the output directories. Each ZIP can be tens of MBs, and the Parquet conversion will roughly unpack and recompress the data (usually the Parquet will be smaller than raw). If uploading to S3, ensure your internet connection can handle the data volume.

*(No separate software installation is required for the pipeline itself -- cloning this repository gives you the necessary scripts and Dockerfiles. All Python/R library dependencies are handled within Docker images built at runtime.)*

## Installation

**1. Get the code:** Download or clone this repository to your local machine. If using the Zenodo archive, download the release ZIP and extract it. This will provide a directory (e.g., `hf_eolus_sar_ingestion/`) containing the `scripts/` folder and documentation.

**2. Prepare configuration:** Ensure you have Docker and AWS CLI installed and configured as described above. Verify that the `aws` CLI can list your S3 bucket (`aws s3 ls s3://<your-bucket>` with the chosen profile) and that Athena access is working. Place your Sentinel-1 OCN credentials file in the `scripts/` directory or note its path. If you have a predefined area GeoJSON, place it somewhere accessible and note the path.

**3. (Optional) Adjust script permissions:** The Bash scripts should already have execute permission. If not, run `chmod +x scripts/*.sh` to make them executable. You will run them using `bash` explicitly (as shown below), so this step is usually not needed unless you want to invoke them directly.

There is no additional package installation required. The pipeline is ready to use once the repository is obtained and prerequisites are met. The scripts will automatically build the necessary Docker images on first run (no manual `docker build` needed by the user).

## Usage

The typical workflow involves two steps: **(1) Downloading** the raw Sentinel-1 SAR data for your area/time of interest, and **(2) Ingesting** the downloaded data into GeoParquet + STAC (and uploading to S3/Athena). Below we demonstrate the command-line usage for each step. All commands are run from the repository\'s root or the `scripts/` directory.

### 1. Downloading Sentinel-1 SAR Data

Use the `scripts/download_sar.sh` script to fetch Sentinel-1 SAR OCN products (OWI files) within a date range and geographic boundary. At minimum you must specify the start date, end date, and a boundary GeoJSON file. The script will prompt an error if required arguments are missing. Example usage:

    bash scripts/download_sar.sh \
      --start-date 2024-01-01 --end-date 2024-01-03 \
      --boundary-file path/to/area_boundary.geojson \
      --credentials-file ~/.config/s1ocn.txt \
      --buffer-km 5 \
      --output-dir /tmp/sar_downloads
    ```[21]

    In this example, data from **January 1, 2024** through **January 3, 2024** are requested for the region defined in `area_boundary.geojson`. We specify a credentials file (`~/.config/s1ocn.txt`) containing our Sentinel-1 OCN login, and apply a 5 km buffer around the input polygon (useful if you want to include scenes just beyond your area)[22][23]. The output ZIP files will be saved under `/tmp/sar_downloads` (if not given, the default is `scripts/downloads/` within the repo)[24][25].

    **What the download script does:** It will build a Docker image (named `download_sar_pipeline:latest`) using the provided Dockerfile, which includes R and the necessary packages[26]. It then runs the container, mounting the output directory and your input files. Inside the container, the R script `download_SAR.R` executes the following steps[3]:

    - Reads the **credentials** for the Sentinel-1 API (username & password) and authenticates with the OCN data service[4].
    - Reads the **boundary GeoJSON**, optionally applies the buffer (5 km in the example), and computes the bounding box of the area[27].
    - Searches the Sentinel-1 catalog for all **OCN products** (wind field data) that overlap the area and date range.
    - Creates a CSV listing all candidate files (`files_to_download.csv`) and then downloads each available ZIP file into the output directory, skipping any that are already present[4].
    - Writes a `downloaded_files.txt` which contains the full paths of the successfully downloaded ZIP files[4].

    After the script finishes, check the output directory (`/tmp/sar_downloads` in the example). You should see the Sentinel-1 *.zip files for each scene, as well as a **`files_to_download.csv`** (all scenes found) and **`downloaded_files.txt`** (scenes actually downloaded)[28]. The CSV is useful for reviewing metadata (e.g., timestamps, identifiers of scenes), and the TXT is useful for passing the list of files to other processes.

    **Resuming downloads:** The download pipeline is designed to be **resumable**. If the process is interrupted or a network glitch causes a failure, you can simply re-run the `download_sar.sh` with the **same parameters and output directory**, and it will **skip files that were already downloaded** and attempt any that were missing[29]. (The script checks existing files and only fetches new ones or retries failed ones, so you won’t re-download everything from scratch.) However, note that not all errors are automatically handled – if an unexpected error occurs, the script may stop. It’s recommended to monitor the download job rather than running it completely unattended, especially for large date ranges[29].

    ### 2. Ingesting Data to GeoParquet & STAC

    Once you have a set of downloaded SAR OCN ZIP files, use the `scripts/ingest_sar.sh` script to process them into Parquet and create the STAC catalog. This script requires a bit more configuration: you must specify an S3 location and AWS details for output, as well as an identifier for the STAC Collection. Basic usage:

    ```bash
    bash scripts/ingest_sar.sh \
      --s3-uri s3://my-bucket/sar_data/ --profile default \
      --athena-db my_database --athena-table sar_owi \
      --collection s1-owi \
      --data-dir /tmp/sar_downloads --output-dir /tmp/catalog_output
    ```[30][31]

    In this example, we point the script to an S3 bucket (`s3://my-bucket/sar_data/`) where the output will be stored, and use the AWS CLI `default` profile for authentication[32]. We specify an Athena database (`my_database`) and table name (`sar_owi`) to use for the external table that will be created[33]. The `--collection` argument sets the STAC Collection ID to **"s1-owi"** (short for Sentinel-1 Ocean Wind Imagery, for instance) – you can choose any identifier here, ideally something descriptive and unique for this dataset. We also provide the path to the directory containing the downloaded ZIPs (`--data-dir /tmp/sar_downloads`, which is the output from the previous step) and an output directory for the local results (`--output-dir /tmp/catalog_output`). The output directory is where the Parquet files and STAC JSONs will be written on your local filesystem before upload; by default it’s `scripts/catalog_output/` if not specified[34][35].

    **What the ingest script does:** After parsing the arguments, `ingest_sar.sh` will perform the following major steps[6][5]:

    1. **Build and run the Parquet conversion container:** The script uses `Dockerfile.upload` to build an image (`ingest_sar_pipeline:latest`) that contains R and required libraries (notably the custom `S1OCN` R package for reading the OCN data, as well as `sf`, `arrow`, `dplyr`, etc.)[36]. It also installs Python dependencies and AWS CLI inside the container, enabling R to use AWS if needed via the `reticulate` package[37]. The script then **runs the container**, executing the `upload_SAR.R` script inside it[6]. This R script looks into the specified `--data-dir` (mounted as `/app/downloads` in the container) for ZIP files and processes them:
       - It reads each ZIP and extracts the **ocean wind data** using functions provided by the `S1OCN` package[38]. This yields a table of wind observations (e.g., wind speed, direction, location, time, etc.) for each scene.
       - It adds a new column for **date** (YYYY-MM-DD) derived from each observation’s timestamp[39]. This will be used as a partition key.
       - It writes out the data as a **partitioned GeoParquet dataset**, partitioning by the date column. The output Parquet files are organized in a directory structure under `--output-dir` (e.g., `assets/date=YYYY-MM-DD/part-*.parquet`). Each Parquet file includes embedded geospatial metadata (GeoParquet v1.1.0 spec) describing the coordinate reference system and geometry column[40][41]. The geometry in these files is typically a point location for each wind observation, stored in WKB format along with standard columns for time, wind speed, etc. 
       - The R script also generates a schema definition for Athena: it outputs a `columns.sql` file listing the column names and types of the Parquet dataset (Athena’s DDL)[42]. This is used later to create the Athena table.
       - A **lineage JSON** (`lineage.json`) is produced, mapping each output date to the source Sentinel-1 file names that contributed to that date’s data[43][44]. This is used to record provenance.

       The result of this container run is a directory (e.g. `/tmp/catalog_output/assets`) full of Parquet files, organized by date, along with the `lineage.json` and the Athena schema file. The Bash script checks that these outputs exist before proceeding.

    2. **Build and run the STAC catalog container:** Next, the script builds a second Docker image (`sar_stac_catalog:latest`) from `Dockerfile.catalog`, which includes Python with **PySTAC** (plus `pyarrow`, `shapely`, etc.)[5]. It then runs `build_sar_catalog.py` inside this container, pointing it to the output directory containing the Parquet files[45]. The Python script scans the `assets/` subfolder and creates one **STAC Item** for each Parquet file[46]. Each STAC Item is a JSON file (under `items/`) that references a Parquet file as an asset, including metadata like:
       - **Spatial footprint** (geometry and bbox) of the data in that file. For a given Parquet (often one day of data), the footprint could be the union or bounding box of all observation points for that day.
       - **Timestamp or time range** (e.g., a datetime if each file is a day’s data, or start-end if applicable).
       - **Properties** such as the platform (Sentinel-1), instrument, processing info, etc. The script attaches any extra properties you provide via `--stac-item-properties-json` or `--stac-collection-properties-json` (you can supply JSON files with additional fields to merge into each Item or the Collection).
       - **Table Schema**: using STAC’s **Table Extension**, each Item (and the Collection) is enriched with a description of the tabular data schema (fields, types) present in the Parquet asset[46]. This allows consumers to see what columns (e.g., `wind_speed`, `wind_direction`, etc.) are in the data without opening the file.
       - **Links and Collection**: The script creates a single **STAC Collection** JSON (saved as `collection.json` in the output) that summarizes the dataset as a whole (overall spatial extent, temporal range, license, keywords, provider info, etc.). All Items are linked to this Collection, and the Collection links back to its Items. The Collection ID is the one you provided via `--collection`.

       The STAC generation step also **validates** the catalog structure using PySTAC’s validation (to ensure compliance with STAC schema)[46], and normalizes all HREFs. After this step, you will have a populated `items/` directory with many Item JSON files and a `collection.json` describing the whole set.

    3. **Augment metadata with lineage:** After building the STAC catalog, `ingest_sar.sh` adds a **processing lineage** to each STAC Item. It uses the earlier `lineage.json` (if available) to find which Sentinel-1 source file names contributed to each Parquet item, and writes that into a `processing:lineage` property in the Item metadata[47][48]. It also adds the STAC **Processing Extension** reference to the item. This means each Item carries provenance info (e.g., *"Derived from Sentinel-1 Level-2 OCN OWI products A,B,C using ingest_sar.sh (doi:...)"*)[13]. If lineage info is missing, it still adds a generic note about the processing. Once this is done, the temporary `lineage.json` is removed.

    4. **Upload to S3:** The script now takes the entire output directory (Parquet files in `assets/`, `items/` folder, `collection.json`) and **copies it to the specified S3 URI** using `aws s3 cp --recursive`[49]. After this, your S3 bucket will have a folder (e.g., `s3://my-bucket/sar_data/`) containing the `assets` (Parquet files) and the STAC catalog (the JSON files). The Parquet files on S3 retain the partitioned structure by date, which is ideal for querying.

    5. **Athena table setup:** Finally, the script uses the AWS CLI to configure Athena for querying the Parquet data. It first ensures the Athena **database** exists (by running a `CREATE DATABASE IF NOT EXISTS` query)[50]. Then it constructs a `CREATE EXTERNAL TABLE` DDL statement for the specified table name, using the column definitions from the earlier `columns.sql` (excluding the partition column)[51]. The table is defined pointing to the S3 location of the Parquet files (the `assets/` prefix) and is partitioned by the `date` field (which was added as an extra partition column)[51][52]. The script submits this DDL via Athena (using `aws athena start-query-execution`) and waits for it to succeed[51][53]. If an old table with the same name exists, it drops it first to avoid errors[54]. After creating the table, the script runs an **Athena MSCK REPAIR TABLE** command to load the partitions (so Athena is aware of each date partition in the S3 data)[55]. This allows Athena to recognize all the Parquet files under `assets/` by their date partition. Logs of these Athena steps are saved under `scripts/` (e.g., `athena_create_table.sql`, `athena_repair_table.sql` for the statements, and Athena query execution IDs are logged).

    Once `ingest_sar.sh` completes successfully, your data is available in multiple forms:

    - **Local Output Directory:** The specified `--output-dir` (e.g., `/tmp/catalog_output`) will contain an **`assets/`** folder with GeoParquet files partitioned by date (one or more Parquet files per day, each embedding geospatial metadata)[56], an **`items/`** folder with STAC Item JSONs, and a **`collection.json`** file representing the STAC Collection[56]. You can inspect these files locally (e.g., open a JSON to see metadata, or read a Parquet with Pandas/GeoPandas).

    - **S3 Bucket:** The same structure is now mirrored on S3 (under the `s3://my-bucket/sar_data/` prefix in the example). This makes the data accessible to cloud workflows – you could, for instance, load the Parquet files directly into a Pandas DataFrame over S3 using `pyarrow`, or register the S3 path in a different analytics service. The STAC catalog on S3 can be made public or shared, allowing others to discover the data via STAC index or viewers (the catalog is static, just a set of JSONs).

    - **Athena Table:** The AWS Athena table **`my_database.sar_owi`** is now created and partitioned. You can query it using SQL (e.g., via AWS Console, AWS CLI, or any Athena API). For example: 

      ```sql
      SELECT date, AVG(wind_speed) 
      FROM my_database.sar_owi 
      WHERE date BETWEEN '2024-01-01' AND '2024-01-31' 
        AND some_spatial_column = '...'
      GROUP BY date;
      ``` 

      (Replace `some_spatial_column` with actual columns; you might have columns like latitude, longitude, wind speed, direction, etc., depending on schema.) Athena will read the Parquet files from S3 to answer queries. Because the data is partitioned by date, queries that filter by date will only read the relevant files, making it efficient. All geospatial columns are stored as well (for example, if the Parquet contains a geometry or WKB column, it is stored as binary in Athena; spatial queries would require Athena GIS support or pre-filtering by bbox etc., since Athena cannot natively understand WKB). 

    - **STAC Catalog:** You can use any STAC-compatible tool to explore the catalog. For instance, with [PySTAC](https://pystac.readthedocs.io/), you could load the collection: 

      ```python
      import pystac
      catalog = pystac.Catalog.from_file('s3://my-bucket/sar_data/collection.json')
      items = [item for item in catalog.get_all_items()]
      print(f"Loaded {len(items)} items from the STAC catalog.")
      ``` 

      Each item will have the asset URL pointing to the Parquet file on S3, and you can see the metadata like datetime, bounding box, and table schema in the item properties.

    ## Example Workflow

    To tie everything together, here is a concise example workflow:

    **Step 1: Download data for a region and time window.**

    Suppose we want all Sentinel-1 OCN wind data over a certain area (say, a coastal region defined in `region.geojson`) for January 2024. We have our credentials in `~/s1ocn_credentials.txt`. Run:

    ```bash
    bash scripts/download_sar.sh \
      --start-date 2024-01-01 --end-date 2024-01-31 \
      --boundary-file region.geojson \
      --credentials-file ~/s1ocn_credentials.txt \
      --output-dir /data/sar_raw

This will create `/data/sar_raw/` and populate it with the ZIP files for all Sentinel-1 OCN scenes in January 2024 covering the area in `region.geojson`. It will also produce `files_to_download.csv` and `downloaded_files.txt` in that folder for reference.

**Step 2: Ingest the downloaded data into GeoParquet and STAC, upload to cloud.**

Now we convert the raw data to Parquet and push to an S3 bucket for analysis. We'll use bucket name `my-hfeolus-bucket` in region `eu-west-1`, and set the STAC collection ID to `sentinel1-owi`. Run:

    bash scripts/ingest_sar.sh \
      --s3-uri s3://my-hfeolus-bucket/sentinel1_owi_jan2024/ \
      --profile default --region eu-west-1 \
      --athena-db eolus_data --athena-table sentinel1_owi_jan2024 \
      --collection sentinel1-owi \
      --data-dir /data/sar_raw --output-dir /data/sar_catalog

This will build Docker images and process the data. After completion, the Parquet files and STAC catalog are in `/data/sar_catalog` locally, and also uploaded to `s3://my-hfeolus-bucket/sentinel1_owi_jan2024/`. An Athena table `eolus_data.sentinel1_owi_jan2024` is created (if the DB `eolus_data` didn't exist, it's created). We can now query this table in Athena (each row corresponds to a wind observation from Sentinel-1). We can also open the STAC `collection.json` (on S3 or locally) to inspect metadata, or use a STAC Browser to visualize the spatial extent of items.

## GeoParquet and STAC Data Specification (Brief)

**GeoParquet format:** The Parquet files produced follow the **GeoParquet v1.1.0** specification[\[9\]]. In each Parquet file, geospatial columns (e.g., geometry or coordinates) are stored with metadata in the file's schema. Each file's metadata contains a top-level `geo` object that declares the GeoParquet version and details each geometry column (its encoding, geometry types, CRS, bounding box, etc.)[\[40\]][][\[57\]]. The pipeline ensures that the geometry data (likely point locations for wind vectors) are stored in WKB and the coordinate reference system is documented (defaulting to WGS84 lat/lon). GeoParquet makes the data self-describing -- any tool that understands GeoParquet can read these files and immediately know how to interpret the spatial data. This is crucial for interoperability and efficient analysis: for example, you can load these Parquet files with **GeoPandas** or **GDAL** and the geometries will be recognized with the correct CRS[\[58\]]. Storing data in Parquet also provides excellent compression and the ability to do columnar queries (scan only needed columns), which is advantageous for large datasets (e.g., you might query just timestamps and wind speeds without reading geometry at all)[\[59\]].

**STAC catalog:** The metadata for the dataset is provided as a static **STAC catalog** (compliant with STAC 1.0.0). This includes a single **Collection** JSON and multiple **Item** JSONs. The **STAC Collection** describes the entire dataset of Sentinel-1 OWI-derived observations -- it contains high-level metadata such as the dataset's spatial extent (bounding box covering all data) and temporal extent (earliest to latest observation), a description, licensing information, and references to the schema of the data (via the Table Extension) and the coordinate reference system used[\[60\]][][\[61\]]. It also links to all the Item entries. Each **STAC Item** corresponds to one partition of data (in this pipeline, typically one day's Parquet file) and is structured as a GeoJSON Feature[\[62\]]. It has its own geometry (e.g., the convex hull or bounding box of all points for that day), a timestamp (or interval) indicating the data time, and a list of asset links. The primary asset of each Item is the Parquet file for that date, with a media type like `"application/x-parquet"` and extra fields (via **STAC Table Extension**) describing its contents (column names/types)[\[46\]]. The Items and Collection also include the **Processing Extension** fields: we tag each with `processing:level`, `processing:software` (identifying this pipeline and version), and `processing:lineage` (listing source Sentinel-1 filenames) for transparency[\[48\]]. By using STAC, our catalog can be crawled or searched by existing tools; for example, one could query the catalog for all Items covering a certain sub-region and date range, then retrieve the corresponding Parquet files for analysis. The STAC metadata makes the dataset discoverable and **reusable**, without requiring a database or custom API -- the catalog can simply live on disk or S3 and be read by clients as JSON[\[63\]].

**Reference Specifications:** For more details on the data formats, please refer to the **HF-EOLUS GeoParquet & STAC Specifications** repository[\[64\]]. That documentation provides in-depth descriptions of how geometry and CRS are encoded in our Parquet files and how STAC is used to organize the assets. In summary, this pipeline adheres to open standards to maximize compatibility: GeoParquet for efficient storage of geospatial data, and STAC for standardized metadata and cataloging[\[14\]].

## Additional Notes

-   **Cleaning up:** If you re-run the ingestion on the same output directory, the script by default will **delete the previous output** (unless `--keep-output` is used)[\[65\]]. This ensures stale data doesn't mix with new results. Similarly, on S3 the uploaded files will overwrite if they have the same names. If you want to append new data to an existing catalog (e.g., add a new month of data), you may need to combine catalogs or use separate collection IDs -- the current scripts are designed for batch, one-off runs producing a fresh dataset/catalog each time (not incremental updates).

-   **Parallel processing:** The R processing (`upload_SAR.R`) uses parallel workers for extracting wind data (`workers=10` by default in S1OCN)[\[66\]]. If running on a multi-core system, the Docker container will utilize multiple cores to speed up the extraction. Ensure your Docker has access to enough CPUs and memory for large jobs. The Athena queries at the end are executed sequentially (database creation, table creation, repair).

-   **Logs:** Both scripts output informative logs to the console and also save logs in the `scripts/logs/` directory (with timestamps in filenames) for ingestion[\[67\]]. If something goes wrong, check these logs for details. The Athena query results or errors can often be retrieved via the AWS Console as well, using the logged QueryExecutionId.

-   **Using other storage or query engines:** While the pipeline is built around S3 and Athena, the core outputs (Parquet files and STAC metadata) are not tied to AWS. You could upload the Parquet files to another cloud storage or load them into a local database. Likewise, the STAC catalog can be placed on a web server or storage bucket of your choice. Athena is used for convenience to automatically register the data for SQL querying; other SQL engines (e.g., Presto/Trino, Spark SQL, or even Python with DuckDB) can also directly query the Parquet dataset using the partitioned files. You would just need to manually set those up if not using Athena.

-   **Documentation:** More detailed documentation for each stage of the pipeline is available in the [`docs/`] folder of this repository. In particular, see the [**Download Pipeline guide**] and the [**Ingestion Pipeline guide**] for a deep dive into the implementation and additional examples. These documents provide expanded explanations of the scripts' internals and options.

By following the above steps and guidelines, researchers can incorporate Sentinel-1 SAR ocean wind data into their workflows with ease -- the pipeline automates the heavy lifting of data retrieval, conversion to analysis-ready format, and metadata generation. The combination of GeoParquet and STAC ensures the resulting dataset is not only efficient to work with at scale, but also **self-described and shareable** under open standards, which is ideal for scientific collaboration and reproducibility.

[\[1\]][] [\[5\]][] [\[6\]][] [\[8\]][] [\[15\]][] [\[30\]][] [\[31\]][] [\[32\]][] [\[33\]][] [\[36\]][] [\[42\]][] [\[46\]][] [\[56\]] ingest\_sar\_pipeline.md

<https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md>

[\[2\]][] [\[3\]][] [\[4\]][] [\[7\]][] [\[16\]][] [\[17\]][] [\[18\]][] [\[19\]][] [\[20\]][] [\[21\]][] [\[22\]][] [\[23\]][] [\[26\]][] [\[27\]][] [\[28\]][] [\[29\]] download\_sar\_pipeline.md

<https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md>

[\[9\]][] [\[40\]][] [\[41\]][] [\[57\]][] [\[58\]][] [\[59\]] geoparquet\_specs.md

<https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md>

[\[10\]][] [\[11\]][] [\[12\]][] [\[60\]][] [\[61\]][] [\[62\]][] [\[63\]] stac\_specs.md

<https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md>

[\[13\]][] [\[34\]][] [\[35\]][] [\[45\]][] [\[47\]][] [\[48\]][] [\[49\]][] [\[50\]][] [\[51\]][] [\[52\]][] [\[53\]][] [\[54\]][] [\[55\]][] [\[65\]][] [\[67\]] ingest\_sar.sh

<https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh>

[\[14\]][] [\[64\]] README.md

<https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/README.md>

[\[24\]][] [\[25\]] download\_sar.sh

<https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/download_sar.sh>

[\[37\]][] [\[38\]][] [\[39\]][] [\[43\]][] [\[44\]][] [\[66\]] upload\_SAR.R

<https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R>

  [\[1\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L3-L5
  [\[2\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L4-L7
  [\[3\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L27-L35
  [\[4\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L14-L17
  [\[5\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L26-L31
  [\[6\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L14-L19
  [\[7\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L20-L25
  [\[8\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L20-L25
  [\[9\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md#L7-L15
  [\[10\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L9-L17
  [\[11\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L15-L23
  [\[12\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L17-L20
  [\[13\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L248-L257
  [\[14\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/README.md#L3-L5
  [\[15\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L6-L9
  [\[16\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L8-L11
  [\[17\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L8-L13
  [\[18\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L2-L5
  [\[19\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L10-L13
  [\[20\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L56-L64
  [\[40\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md#L9-L17
  [\[57\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md#L13-L16
  [\[58\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md#L17-L19
  [\[59\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md#L22-L29
  [\[60\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L11-L19
  [\[61\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L53-L61
  [\[62\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L7-L15
  [\[46\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L2-L5
  [\[48\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L250-L258
  [\[63\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/stac_specs.md#L13-L21
  [\[64\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/README.md#L3-L6
  [\[65\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L42-L50
  [\[66\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R#L128-L135
  [\[67\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L164-L168
  [`docs/`]: docs/
  [**Download Pipeline guide**]: docs/download_sar_pipeline.md
  [**Ingestion Pipeline guide**]: docs/ingest_sar_pipeline.md
  [\[30\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L38-L46
  [\[31\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L72-L80
  [\[32\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L48-L56
  [\[33\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L50-L54
  [\[36\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L20-L28
  [\[42\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L22-L25
  [\[56\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/ingest_sar_pipeline.md#L64-L69
  [\[21\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L88-L96
  [\[22\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L44-L49
  [\[23\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L58-L65
  [\[26\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L20-L28
  [\[27\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L28-L36
  [\[28\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L98-L101
  [\[29\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/docs/download_sar_pipeline.md#L102-L107
  [\[41\]]: https://github.com/GOFUVI/hf_eouls_geoparquet_stac_specs/blob/5866885c54a5a941989b10387611d7f45c064dbd/geoparquet_specs.md#L13-L19
  [\[34\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L28-L34
  [\[35\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L50-L58
  [\[45\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L229-L237
  [\[47\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L238-L247
  [\[49\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L286-L294
  [\[50\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L314-L323
  [\[51\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L294-L302
  [\[52\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L300-L305
  [\[53\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L313-L321
  [\[54\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L342-L345
  [\[55\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/ingest_sar.sh#L2-L10
  [\[24\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/download_sar.sh#L20-L24
  [\[25\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/download_sar.sh#L30-L38
  [\[37\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R#L32-L40
  [\[38\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R#L127-L135
  [\[39\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R#L138-L141
  [\[43\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R#L76-L84
  [\[44\]]: https://github.com/GOFUVI/hf_eolus_sar_ingestion/blob/8c178ab84db99be6a873526b287e324481b38062/scripts/upload_SAR.R#L90-L99
