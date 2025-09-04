#!/usr/bin/env Rscript
# Script to process downloaded SAR data and generate a GeoParquet dataset partitioned by date
# Prerequisite: download_SAR.sh must be run to populate 'downloads/' with SAR ZIP files.
library(magrittr)
library(sf)
# --- Logging utilities and error handler ---
log_info <- function(msg, ...) message(sprintf("[INFO] %s", sprintf(msg, ...)))
log_warn <- function(msg, ...) warning(sprintf("[WARN] %s", sprintf(msg, ...)), call. = FALSE)
log_debug <- function(msg, ...) message(sprintf("[DEBUG] %s", sprintf(msg, ...)))
options(error = function() {
  cat("\n[ERROR] Unhandled error. Printing traceback...\n", file = stderr())
  try(traceback(), silent = TRUE)
  quit(status = 1)
})
if (requireNamespace("future", quietly = TRUE)) {
  future::plan("sequential")
}
library(arrow)
library(dplyr)
library(stringr)
library(digest)
library(bit64)
# Ensure S1OCN package is available (installed via Dockerfile.upload build)
if (!requireNamespace("S1OCN", quietly = TRUE)) {
  stop(
    "R package 'S1OCN' not found. Please run inside the Docker container built by build_and_run_upload_container.sh",
    call. = FALSE
  )
}
library(S1OCN)
library(reticulate)
log_info("R version: %s", R.version.string)
# Configure reticulate to use the container's Python interpreter
use_python(Sys.getenv("RETICULATE_PYTHON"), required = TRUE)
log_info("RETICULATE_PYTHON: %s", Sys.getenv("RETICULATE_PYTHON"))
# Verify required Python modules for GeoParquet; expect to run inside Docker with dependencies pre-installed
required_py_pkgs <- c("pandas", "pyarrow", "geopandas")
lapply(required_py_pkgs, function(pkg) {
  if (!py_module_available(pkg)) {
    stop(sprintf(
      "Python module '%s' not found. Please run inside Docker container built by build_and_run_upload_container.sh",
      pkg
    ), call. = FALSE)
  }
})
try({
  log_info("Python package versions: pyarrow=%s, pandas=%s, geopandas=%s",
           import("pyarrow")$`__version__`,
           import("pandas")$`__version__`,
           import("geopandas")$`__version__`)
}, silent = TRUE)
log_info("R packages: sf=%s, arrow=%s, dplyr=%s",
         as.character(utils::packageVersion("sf")),
         as.character(utils::packageVersion("arrow")),
         as.character(utils::packageVersion("dplyr")))

#' Write lineage mapping for GeoParquet outputs
#'
#' @description Write a JSON file that maps each observation date to the input
#'   Sentinel-1 product files contributing to that date's GeoParquet output.
#'
#' @param wind_df A data frame containing wind observations with columns
#'   `date` and `Name`.
#' @param output_dir Directory where the lineage JSON file will be written.
#'
#' @details The function groups records in `wind_df` by `date` and collects the
#'   unique `Name` values. The mapping is saved as `lineage.json` inside
#'   `output_dir` with dates as keys and arrays of file names as values.
#'
#' @return Invisibly returns the path to the created JSON file.
#'
#' @examples
#' df <- data.frame(date = c("2020-01-01", "2020-01-01", "2020-01-02"),
#'                  Name = c("A.zip", "B.zip", "C.zip"))
#' tmp <- tempdir()
#' write_lineage_map(df, tmp)
#' file.exists(file.path(tmp, "lineage.json"))
write_lineage_map <- function(wind_df, output_dir, zip_files = NULL) {
  lineage_path <- file.path(output_dir, "lineage.json")

  build_from_wind <- !is.null(wind_df) && all(c("date", "Name") %in% names(wind_df))
  if (build_from_wind) {
    lineage_df <- wind_df %>%
      dplyr::group_by(date) %>%
      dplyr::summarise(files = list(unique(Name)), .groups = "drop")
    lineage_list <- setNames(lineage_df$files, lineage_df$date)
    jsonlite::write_json(lineage_list, lineage_path, auto_unbox = TRUE)
    log_info("Lineage mapping written (from wind_data) to %s", lineage_path)
    return(invisible(lineage_path))
  }

  # Fallback: derive lineage by parsing dates from ZIP filenames
  if (!is.null(zip_files) && length(zip_files) > 0) {
    bn <- basename(zip_files)
    # Extract first 8-digit date (YYYYMMDD) token from filename
    d_raw <- stringr::str_extract(bn, "(?<!\\d)\\d{8}(?!\\d)")
    # Convert to YYYY-MM-DD; suppress warnings for unparsable entries
    suppressWarnings({ dates <- as.Date(d_raw, format = "%Y%m%d") })
    keep <- !is.na(dates)
    if (!any(keep)) {
      log_warn("Cannot write lineage map; no parseable YYYYMMDD dates found in ZIP filenames")
      return(invisible(NULL))
    }
    df <- data.frame(date = format(dates[keep], "%Y-%m-%d"), file = bn[keep], stringsAsFactors = FALSE)
    lineage_list <- split(df$file, df$date)
    jsonlite::write_json(lineage_list, lineage_path, auto_unbox = TRUE)
    log_info("Lineage mapping written (from ZIP filenames) to %s", lineage_path)
    return(invisible(lineage_path))
  }

  log_warn("Cannot write lineage map; columns 'date'/'Name' missing and no ZIP list provided")
  invisible(NULL)
}

 #' Compute deterministic row identifiers
 #' 
 #' @description Generate a stable 64-bit identifier for each observation using a
 #'   fast hash of time, longitude, latitude and wind speed.
 #' 
 #' @param firstMeasurementTime POSIXct vector of measurement times.
 #' @param lon Numeric vector of longitudes (degrees).
 #' @param lat Numeric vector of latitudes (degrees).
 #' @param owiWindSpeed Numeric vector of wind speeds (m/s).
 #' 
 #' @details Optimized for speed by avoiding per-row serialization of geometry
 #'   objects. Builds a canonical string key per row using: UNIX time in seconds
 #'   (UTC), full-precision longitude, latitude, and wind speed formatted with 17
 #'   significant digits to preserve double precision. Each key is hashed with
 #'   `xxhash64` via the `digest` package using `serialize = FALSE`.
 #' 
 #' @return A character vector of 16-character lowercase hex strings representing
 #'   the 64-bit hash. These will be converted to signed int64 in the Python
 #'   writer to be stored as BIGINT in Parquet/Athena.
 compute_rowid <- function(firstMeasurementTime, lon, lat, owiWindSpeed) {
   n <- length(firstMeasurementTime)
   stopifnot(n == length(lon), n == length(lat), n == length(owiWindSpeed))
 
   # Canonical string representation (vectorized)
   ts  <- as.integer(as.POSIXct(firstMeasurementTime, tz = "UTC"))
   lon_s <- sprintf("%.17g", lon)
   lat_s <- sprintf("%.17g", lat)
   spd_s <- sprintf("%.17g", owiWindSpeed)
   keys <- paste(ts, lon_s, lat_s, spd_s, sep = "|")
 
   # Fast non-serializing hashing; returns hex string per key
   vapply(keys, digest::digest, FUN.VALUE = character(1),
          algo = "xxhash64", serialize = FALSE)
 }

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 0) {
  stop("Usage: Rscript upload_SAR.R", call. = FALSE)
}

download_dir <- "downloads"

zip_files <- list.files(download_dir, pattern = "\\.zip$", full.names = TRUE)
log_info("ZIP files found: %d", length(zip_files))
if (length(zip_files) > 0) log_debug("First ZIP: %s", basename(zip_files[1]))
if (length(zip_files) == 0) {
  stop("No zip files found in downloads directory")
}

# Extract wind data from downloaded files
SAR_entries <- data.frame(Name = basename(zip_files), downloaded_file_path = zip_files, stringsAsFactors = FALSE)
log_info("Extracting wind data from %d entries via S1OCN...", nrow(SAR_entries))
wind_data <- S1OCN::s1ocn_extrat_wind_data_from_files(SAR_entries, workers = 10)
wind_data <- wind_data %>%
  S1OCN::s1ocn_wind_data_list_to_tables(workers = 1) %>%
  bind_rows() %>%
  arrange(firstMeasurementTime)
log_info("wind_data rows: %d, columns: %d", nrow(wind_data), ncol(wind_data))
log_debug("Columns: %s", paste(names(wind_data), collapse = ", "))

# Add date partitioning column formatted as YYYY-MM-DD
log_info("Adding date partition column from firstMeasurementTime...")
wind_data <- wind_data %>%
  mutate(date = format(as.Date(firstMeasurementTime), "%Y-%m-%d"))

# Write GeoParquet dataset partitioned by date using Python via reticulate
output_dir <- "parquet_output"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
# Construct geometry column from longitude/latitude for GeoParquet
lon_col <- grep("lon", names(wind_data), ignore.case = TRUE, value = TRUE)
lat_col <- grep("lat", names(wind_data), ignore.case = TRUE, value = TRUE)
log_info("Detected coordinate columns: lon='%s', lat='%s'", paste(lon_col, collapse=","), paste(lat_col, collapse=","))
# Convert coordinates to an sf geometry column for GeoParquet
if (length(lon_col) == 1 && length(lat_col) == 1) {
  wind_data <- sf::st_as_sf(wind_data, coords = c(lon_col, lat_col), crs = 4326, remove = FALSE)
  log_info("Constructed geometry from columns '%s' and '%s'", lon_col, lat_col)
} else {
  stop("Cannot find longitude/latitude columns to construct geometry for GeoParquet")
}
# Extract the geometry as sfc for conversion
geom_sfc <- sf::st_geometry(wind_data)
# Convert geometry to WKB binary and store WKT for text-based exports
wind_data$geometry <- sf::st_as_binary(geom_sfc)
# Compute deterministic rowid values (hex strings) using time + lon/lat + wind speed
wind_data <- wind_data %>%
  mutate(rowid = compute_rowid(firstMeasurementTime, .data[[lon_col]], .data[[lat_col]], owiWindSpeed))
log_info("Computed deterministic rowid values for %d rows", nrow(wind_data))
# Compute bounding box for GeoParquet metadata
bbox <- sf::st_bbox(geom_sfc)
log_info("Computed bbox: minx=%.6f, miny=%.6f, maxx=%.6f, maxy=%.6f", bbox[[1]], bbox[[2]], bbox[[3]], bbox[[4]])

# Export to GeoParquet with explicit metadata using PyArrow (keep as Python objects)
pa <- import("pyarrow", convert = FALSE)
ds <- import("pyarrow.dataset", convert = FALSE)
# Keep JSON return values as Python objects to allow .encode()
json <- import("json", convert = FALSE)

# Convert the R sf object to a pandas DataFrame for PyArrow
df_py <- r_to_py(wind_data)
# Convert 'rowid' from hex string to signed int64 in pandas (robust membership check)
try({
  reticulate::py_run_string("\nimport numpy as np\n\nMASK63 = (1 << 63) - 1\n\n\ndef _hexseries_to_int64_pos(s):\n    vals = s.astype(str)\n    def conv(x):\n        if x is None:\n            return np.int64(0)\n        x = x.strip()\n        if not x:\n            return np.int64(0)\n        if x.startswith('0x') or x.startswith('0X'):\n            x = x[2:]\n        iv_u = int(x, 16)\n        iv = iv_u & MASK63\n        return np.int64(iv)\n    return vals.map(conv)\n")
  cols <- try(py_to_r(df_py$columns$tolist()), silent = TRUE)
  if (!inherits(cols, "try-error") && "rowid" %in% cols) {
    df_py$`__setitem__`("rowid", reticulate::py$`_hexseries_to_int64_pos`(df_py$`__getitem__`("rowid")))
    # Ensure plain int64 dtype
    df_py$`__setitem__`("rowid", df_py$`__getitem__`("rowid")$astype("int64"))
    dtype_name <- try(py_to_r(df_py$`__getitem__`("rowid")$dtype$name), silent = TRUE)
    try({ log_info("pandas dtype for 'rowid' after conversion: %s", as.character(dtype_name)) }, silent = TRUE)
  } else {
    log_warn("'rowid' column not found in pandas DataFrame; it may be missing or renamed")
  }
}, silent = TRUE)
tbl <- pa$Table$from_pandas(df_py, preserve_index = FALSE)
# Force-cast 'rowid' field to int64 at Arrow level as a final safeguard
try({
  reticulate::py_run_string("\nimport pyarrow as pa\nimport pyarrow.compute as pc\n\ndef _ensure_rowid_int64(tbl):\n    try:\n        i = tbl.schema.get_field_index('rowid')\n    except Exception:\n        return tbl\n    if i == -1:\n        return tbl\n    typ = tbl.schema.field(i).type\n    if pa.types.is_int64(typ):\n        return tbl\n    arr = pc.cast(tbl.column(i), pa.int64())\n    field = tbl.schema.field(i).with_type(pa.int64())\n    return tbl.set_column(i, field, arr)\n")
  tbl <- reticulate::py$`_ensure_rowid_int64`(tbl)
  # Log Arrow dtype of 'rowid'
  try({
    t_str <- reticulate::py_eval("str(tbl.schema.field_by_name('rowid').type)", convert = TRUE)
    log_info("Arrow type for 'rowid': %s", as.character(t_str))
  }, silent = TRUE)
}, silent = TRUE)
log_info("PyArrow table schema fields: %s", paste(py_to_r(tbl$schema$names), collapse = ", "))

log_info("Building CRS84 + GeoParquet metadata objects...")
# Build CRS84 PROJJSON safely without backtick names in list() calls
CRS84 <- list(
  type = "GeographicCRS",
  name = "WGS 84 longitude-latitude",
  datum = list(
    type = "GeodeticReferenceFrame",
    name = "World Geodetic System 1984",
    ellipsoid = list(
      name = "WGS 84",
      semi_major_axis = 6378137,
      inverse_flattening = 298.257223563
    )
  ),
  coordinate_system = list(
    subtype = "ellipsoidal",
    axis = list(
      list(name = "Geodetic longitude", abbreviation = "Lon", direction = "east", unit = "degree"),
      list(name = "Geodetic latitude", abbreviation = "Lat", direction = "north", unit = "degree")
    )
  ),
  id = list(authority = "OGC", code = "CRS84")
)
CRS84[["$schema"]] <- "https://proj.org/schemas/v0.5/projjson.schema.json"

geo_meta <- list(
  version = "1.1.0",
  primary_column = "geometry",
  columns = list(
    geometry = list(
      encoding = "WKB",
      geometry_types = list("Point"),
      crs = CRS84,
      bbox = as.numeric(bbox),
      orientation = "counterclockwise",
      edges = "spherical"
    )
  )
)

log_info("Serializing GeoParquet metadata to JSON via Python json.dumps...")
geo_json <- json$dumps(geo_meta)
log_debug("Geo JSON length (chars): %d", tryCatch(nchar(py_to_r(geo_json)), error = function(e) -1))
log_info("Encoding GeoParquet metadata JSON to UTF-8 bytes...")
geo_bytes <- geo_json$encode("utf-8")

log_info("Preparing schema metadata dictionary (copy existing, if any)...")
schema_meta <- py_eval("dict()", convert = FALSE)
curr_meta <- tbl$schema$metadata
try({ reticulate::py_call(schema_meta$update, list(curr_meta)) }, silent = TRUE)

log_info("Setting 'geo' entry in schema metadata via py_set_item()...")
key_geo <- py_eval("b'geo'", convert = FALSE)
reticulate::py_set_item(schema_meta, key_geo, geo_bytes)

log_info("Creating schema with metadata via a tiny Python helper...")
reticulate::py_run_string("\nimport pyarrow as pa\n\ndef _schema_with_meta(schema, md):\n    return schema.with_metadata(md)\n")
schema_to_use <- tryCatch({
  reticulate::py$`_schema_with_meta`(tbl$schema, schema_meta)
}, error = function(e) {
  log_warn("Failed to attach GeoParquet metadata; falling back to original schema: %s", e$message)
  tbl$schema
})
log_info("Preparing partitioning (hive) for column date...")
part_schema <- pa$schema(list(
  pa$field("date", pa$string())
))
part <- ds$partitioning(part_schema, flavor = "hive")

log_info("Writing GeoParquet dataset to '%s' (partitioned by date)...", output_dir)
ds$write_dataset(
  tbl,
  base_dir = output_dir,
  format = "parquet",
  partitioning = part,
  basename_template = "data_{i}.parquet",
  schema = schema_to_use,
  existing_data_behavior = "overwrite_or_ignore"
)
try({
  files_written <- list.files(output_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  log_info("Files written: %d", length(files_written))
  if (length(files_written) > 0) log_debug("Sample file: %s", basename(files_written[[1]]))
  for (f in files_written) {
    rel <- substring(f, nchar(output_dir) + 2)
    m <- str_match(rel, "date=([0-9]{4}-[0-9]{2}-[0-9]{2})/[^/]+\\.parquet$")
    if (any(is.na(m))) {
      log_warn("Skipping unrecognized Parquet path: %s", rel)
      next
    }
    new_path <- file.path(dirname(f), sprintf("%s.parquet", m[2]))
    if (!file.rename(f, new_path)) {
      log_warn("Failed to rename %s to %s", rel, basename(new_path))
    } else {
      log_debug("Renamed %s to %s", rel, file.path(dirname(rel), basename(new_path)))
    }
  }
}, silent = TRUE)

# Write lineage mapping after dataset write/rename to avoid accidental clobbering
write_lineage_map(wind_data, output_dir, zip_files)

# Export Athena column definitions from Parquet dataset schema for DDL
col_classes <- sapply(wind_data, function(x) class(x)[1])
athena_types <- vapply(col_classes, function(cl) {
  switch(cl,
         integer   = "INT",
         integer64 = "BIGINT",
         numeric   = "DOUBLE",
         character = "STRING",
         POSIXct   = "TIMESTAMP",
         Date      = "DATE",
         logical   = "BOOLEAN",
         list      = "BINARY",
         raw       = "BINARY",
         WKB       = "BINARY",
         stop(sprintf("Unsupported R class for Athena type mapping: %s", cl)))
}, character(1))
# Guarantee BIGINT for rowid regardless of R-side class (converted in pandas)
if ("rowid" %in% names(athena_types)) athena_types[["rowid"]] <- "BIGINT"
col_defs <- paste(sprintf("%s %s", names(athena_types), athena_types), collapse = ", ")
schema_file <- file.path(output_dir, "columns.sql")
writeLines(col_defs, schema_file)
log_info("Athena column definitions written to %s", schema_file)
