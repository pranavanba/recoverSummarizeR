#' Recursively download Synapse files/folders
#'
#' `synget_parquet_to_df()` recursively downloads files (and maintains the original folder structure) from a Synapse
#' directory, reads in the specific types of files it finds (.parquet, .tsv, .csv, .ndjson), and returns a list of data
#' frames. It then removes file extensions and unnecessary prefixes/suffixes from the names of the data frames and
#' filters the list of data frames to include only those data frames contain the string specified by `dataset_name_filter` in
#' their name.
#'
#' @param synDirID A Synapse ID for a folder entity in Synapse.
#' @param dataset_name_filter A string found in the names of the files to be read for use in including only the files that
#'   contain said string in their names.
#'
#' @return A list of data frames whose names contain the string specified by `dataset_name_filter`
#' @export
#' @examples
#' \dontrun{
#' parquetDirectoryID <- "syn12345678"
#' df_list <- synget_parquet_to_df(parquetDirectoryID, "fitbit")
#' # df_list will contain only data frames whose names contain the string "fitbit"
#' }
synget_parquet_to_df <- function(synDirID, dataset_name_filter) {
  system(paste("synapse get -r", synDirID))
  
  file_paths <- list.files(recursive = T, full.names = T)
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
  
  df_list <- df_list[grepl(dataset_name_filter, tolower(names(df_list))) & !grepl("manifest", tolower(names(df_list)))]
  
  return(df_list)
}
