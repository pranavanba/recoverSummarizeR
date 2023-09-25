#' Recursively download Synapse files/folders
#'
#' `syn_dir_to_dflist()` recursively downloads files from a Synapse directory and maintains the original directory
#' structure, reads the dataset/directory files to data frames, and stores the data frames in a list.
#'
#' @param synDirID A Synapse ID for a folder entity in Synapse.
#' @param downloadLocation The location to download files to.
#' @param dataset_name_filter A string used to filter the list of data frames by including only the data frames whose
#'   names contain the specified string.
#'
#' @return A list of data frames. The list is filtered to include only the data frames whose names contain the string
#'   specified by `dataset_name_filter`.
#' @export
#' @examples
#' \dontrun{
#' parquetDirectoryID <- "syn12345678"
#' df_list <- syn_dir_to_dflist(parquetDirectoryID, "fitbit")
#' # df_list will contain only data frames with names containing the string "fitbit"
#' }
syn_dir_to_dflist <- function(synDirID, downloadLocation, dataset_name_filter=NULL) {
  unlink(downloadLocation, recursive = T, force = T)
  system(glue::glue("synapse get -r {synDirID} --manifest suppress --downloadLocation {downloadLocation}"))
  
  file_paths <- list.files(downloadLocation, recursive = T, full.names = T)
  file_paths <- file_paths[grepl("dataset_", (file_paths), ignore.case = T)]
  
  df_list <- lapply(file_paths, function(file_path) {
    if (grepl(".parquet$", file_path)) {
      arrow::read_parquet(file_path)
    } else if (grepl(".tsv$", file_path)) {
      readr::read_tsv(file_path, show_col_types = F)
    } else if (grepl(".ndjson$", file_path)) {
      ndjson::stream_in(file_path, cls = "tbl")
    } else if (grepl(".csv$", file_path)) {
      utils::read.csv(file_path)
    } else {
      stop(paste("Unsupported file format for", file_path))
    }
  })
  
  names(df_list) <- 
    file_paths %>% 
    {paste(basename(dirname(.)), "-", basename(.))} %>% 
    {gsub("\\.(parquet|tsv|ndjson)$|(dataset_|-.*\\.snappy| )", "", .)}
  
  if (!is.null(dataset_name_filter)) {
    pattern <- paste(dataset_name_filter, collapse="|")
    df_list <- df_list[grepl(tolower(pattern), tolower(names(df_list))) & !grepl("manifest", tolower(names(df_list)))]
  }
  
  
  return(df_list)
}
