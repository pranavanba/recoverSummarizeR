# This function downloads files from a Synapse directory, reads in specific types of files (.parquet, .tsv, .ndjson), and returns a list of data frames. It then cleans up the names of the data frames and filters to only include those with "fitbit" in their name.
synget_parquet_to_df <- function(synDirID) {
  system(paste("synapse get -r", synDirID))
  
  file_paths <- list.files(recursive = T, full.names = T)
  file_paths <- file_paths[grepl("dataset_", (file_paths), ignore.case = T)]
  
  df_list <- lapply(file_paths, function(file_path) {
    if (grepl(".parquet$", file_path)) {
      read_parquet(file_path)
    } else if (grepl(".tsv$", file_path)) {
      read_tsv(file_path, show_col_types = F)
    } else if (grepl(".ndjson$", file_path)) {
      ndjson::stream_in(file_path, cls = "tbl")
    } else if (grepl(".csv$", file_path)) {
      read.csv(file_path)
    } else {
      stop(paste("Unsupported file format for", file_path))
    }
  })
  
  names(df_list) <- 
    file_paths %>% 
    {paste(basename(dirname(.)), "-", basename(.))} %>% 
    {gsub("\\.(parquet|tsv|ndjson)$|(dataset_|-.*\\.snappy| )", "", .)}
  
  df_list <- df_list[grepl("fitbit", tolower(names(df_list))) & !grepl("manifest", tolower(names(df_list)))]
  
  return(df_list)
}
