library(magrittr)
library(sf)
# Load parallel futures package and disable parallel execution to avoid FutureLaunchError
if (requireNamespace("future", quietly = TRUE)) {
  future::plan("sequential")
}
library(arrow)

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
# credentials file (first argument), boundary file (second argument), buffer distance in km (third argument)
credentials_file <- if (length(args) >= 1) args[1] else "credentials"
boundary_file   <- if (length(args) >= 2) args[2] else "grids/.cache_grids/area_boundary.geojson"
buffer_km       <- if (length(args) >= 3) as.numeric(args[3]) else 0
# start and end dates are mandatory (fourth and fifth arguments)
if (length(args) >= 5) {
  start_date <- args[4]
  end_date   <- args[5]
} else {
  stop("Usage: download_SAR.R <credentials_file> <boundary_file> <buffer_km> <start_date> <end_date>")
}
cat(sprintf("\n[INFO] Applying buffer of %g km to area boundary\n", buffer_km))

## Read the area boundary from GeoJSON
area_boundary <- sf::st_read(boundary_file, quiet = TRUE)

## Apply buffer to area boundary if specified
buffered_boundary <- if (buffer_km > 0) {
  sf::st_buffer(area_boundary, dist = buffer_km * 1000)
} else {
  area_boundary
}

## Use the bounding box (xmin, ymin, xmax, ymax) of the (buffered) boundary as the search polygon
search_polygon <- sf::st_bbox(buffered_boundary)

search_polygon %<>% sf::st_as_sfc()

# Display the search polygon to console before downloading
cat("\nSearch polygon bounding box:\n")
print(search_polygon)

## Read S1OCN credentials from credentials file
credentials <- readLines(credentials_file)
username <- credentials[1]
passwd   <- credentials[2]


















rangos_fechas <- list(c(start_date, end_date))

SAR_files <- rangos_fechas %>% purrr::map(\(rango_fechas) {
  S1OCN::s1ocn_list_files(
    attributes_search = list(
      swathIdentifier = "IW",
      orbitDirection = "DESCENDING",
      relativeOrbitNumber = 125
    ),
    datetime_start = rango_fechas[1],
    datetime_end   = rango_fechas[2],
    search_polygon = search_polygon,
    max_results    = Inf
  )
})


	SAR_files %<>% purrr::compact() %>%  dplyr::bind_rows()

# Export list of candidate SAR files before downloading
files_to_download_file <- "files_to_download.csv"
# Drop any list columns (e.g., geometry) for CSV export
files_to_download <- SAR_files %>% dplyr::select(dplyr::where(~ !is.list(.)))
write.csv(files_to_download, file = files_to_download_file, row.names = FALSE)
cat("\nFiles to download written to", files_to_download_file, "\n")

download_dir <- "downloads"
# Ensure downloads directory exists and purge obsolete files
dir.create(download_dir, recursive = TRUE, showWarnings = FALSE)
# Remove any ZIPs in downloads/ not listed for download
existing_zips <- list.files(download_dir, pattern = "\\.zip$", full.names = TRUE)
to_keep <- file.path(download_dir, paste0(SAR_files$Name, ".zip"))
obsolete_zips <- setdiff(existing_zips, to_keep)
if (length(obsolete_zips) > 0) {
  file.remove(obsolete_zips)
  cat("\nRemoved obsolete ZIP files:\n")
  cat(paste(obsolete_zips, collapse = "\n"), "\n")
}
# Create a temporary directory for SAR downloads
temp_zip_dir <- file.path(tempdir(), "SAR_downloads")
dir.create(temp_zip_dir, recursive = TRUE, showWarnings = FALSE)

# Wrap download loop in retry block: on error, wait 5 minutes before retry
repeat {
  download_err <- try({
    while(TRUE) {
      # Clean up temporary partial downloads
      list.files(temp_zip_dir, pattern = "curltmp", full.names = TRUE) %>% file.remove()

      # Determine which files are already fully downloaded
      already_downloaded <- list.files(download_dir, pattern = "\\.zip$", full.names = FALSE) %>%
        stringr::str_remove("\\.zip")
      SAR_files_to_download <- SAR_files %>% dplyr::filter(!Name %in% already_downloaded)
      if (nrow(SAR_files_to_download) == 0) break

      download_dir <- "downloads"
      dir.create(download_dir, recursive = TRUE, showWarnings = FALSE)

      max_attempts <- 5
      file_max_attempts <- 3
      for (attempt in seq_len(max_attempts)) {
        result <- try({
          nfiles <- nrow(SAR_files_to_download)
          for (i in seq_len(nfiles)) {
            entry <- SAR_files_to_download[i, , drop = FALSE]
            fname <- entry$Name
            zip_path <- file.path(download_dir, paste0(fname, ".zip"))
            if (!file.exists(zip_path)) {
              cat(sprintf("[INFO] Downloading file %d/%d: %s\n", i, nfiles, fname))
              file_attempt <- 1
              repeat {
                file_success <- TRUE
                res <- try(
                  S1OCN:::s1ocn_download_files(entry,
                                               dest = download_dir,
                                               workers = 1,
                                               username = username,
                                               passwd = passwd),
                  silent = TRUE)
                if (inherits(res, "try-error")) {
                  msg_f <- conditionMessage(attr(res, "condition"))
                  if (grepl("cannot open the connection", msg_f)) {
                    warning(sprintf("[WARN] readLines failure downloading %s (attempt %d/%d): %s",
                                    fname, file_attempt, file_max_attempts, msg_f))
                    file_success <- FALSE
                    Sys.sleep(5)
                  } else {
                    stop(res)
                  }
                }
                if (file_success) break
                file_attempt <- file_attempt + 1
                if (file_attempt > file_max_attempts) {
                  stop(sprintf("Failed downloading %s after %d attempts due to readLines errors",
                               fname, file_max_attempts))
                }
              }
              Sys.sleep(5)
            }
          }
        }, silent = TRUE)
        if (!inherits(result, "try-error")) break
        msg <- conditionMessage(attr(result, "condition"))
        if (grepl("error: 429", msg)) {
          wait <- attempt * 60
          message(sprintf("Rate limit (429) encountered; retrying in %d seconds...", wait))
          Sys.sleep(wait)
        } else if (grepl("401", msg) || grepl("token", msg, ignore.case = TRUE)) {
          stop(sprintf(
            "Authentication failed (HTTP 401) when fetching token. Please check your credentials file '%s'.\nOriginal error: %s",
            credentials_file,
            msg
          ))
        } else {
          stop(result)
        }
      }
    }
  }, silent = TRUE)
  if (!inherits(download_err, "try-error")) break
  warning("[ERROR] Download pipeline failed; waiting 5 minutes before retry...")
  Sys.sleep(300)
}

# Write downloaded files list
downloaded_list_file <- "downloaded_files.txt"
zip_paths <- list.files(download_dir, pattern = "\\.zip$", full.names = TRUE)
writeLines(zip_paths, con = downloaded_list_file)
quit(save = "no", status = 0)

