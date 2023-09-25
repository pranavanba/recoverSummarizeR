#' Recursively download Synapse files/folders
#'
#' `syn_parquet_dataset_to_dflist()` recursively downloads files from a Synapse directory and maintains the original directory
#' structure, reads the dataset/directory files to data frames, and stores the data frames in a list.
#'
#' @param synDirID A Synapse ID for a folder entity in Synapse.
#' @param method Either 'synapse' or 'sts' to specify the method to use in getting the parquet datasets. 'synapse' will get files directly from a synapse project or folder using the synapse client, while 'sts' will use sts-token access to get objects from an sts-enabled storage location, such as an S3 bucket.
#' @param s3bucket The name of the S3 bucket to access when `method='sts'`.
#' @param s3basekey The base key of the S3 bucket to access when `method='sts'`.
#' @param downloadLocation The location to download files to when `method='sts'`.
#' @param dataset_name_filter A string used to filter the list of data frames by including only the data frames whose
#'   names contain the specified string.
#'
#' @return A list of data frames. The list is filtered to include only the data frames whose names contain the string
#'   specified by `dataset_name_filter`.
#' @export
#' @examples
#' \dontrun{
#' df_list <- syn_parquet_dataset_to_dflist("syn12345678", "sts", "s3://my-bucket/", "main/folder/", "./parquet", "fitbit")
#' # df_list will use the `sts` method to get parquet datasets in synapse at the parquetDirectoryID and the output will contain only data frames for parquet datasets whoes names contain the string "fitbit"
#' }
syn_parquet_dataset_to_dflist <- function(synDirID, method="synapse", s3bucket=NULL, s3basekey=NULL, downloadLocation=NULL, dataset_name_filter=NULL) {
  if (method=="synapse") {
    unlink(downloadLocation, recursive = T, force = T)
    system(glue::glue("synapse get -r {synDirID} --manifest suppress --downloadLocation {downloadLocation}"))
    
    file_paths <- list.files(downloadLocation, recursive = T, full.names = T)
    file_paths <- file_paths[grepl("dataset_", (file_paths), ignore.case = T)]
    
    df_list <- lapply(file_paths, function(file_path) {
      if (grepl("dataset_.*", file_path)) {
        if (grepl(".*fitbit.*intraday.*", file_path)) {
          arrow::open_dataset(file_path) %>% 
            dplyr::select(dplyr::all_of(c("ParticipantIdentifier", 
                                          "DateTime", 
                                          "DeepSleepSummaryBreathRate", 
                                          "RemSleepSummaryBreathRate", 
                                          "FullSleepSummaryBreathRate", 
                                          "LightSleepSummaryBreathRate"))) %>% 
            dplyr::collect()
        } else {
          arrow::open_dataset(file_path) %>% dplyr::collect()
        }
      } else {
        stop(paste("Unsupported file format for files in dataset:", file_path))
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
    
  } else if (method=="sts") {
    synapser::synLogin(authToken = Sys.getenv('SYNAPSE_AUTH_TOKEN'))
    
    token <- synapser::synGetStsStorageToken(
      entity = synDirID,
      permission = "read_only",
      output_format = "json")
    
    if (s3bucket==token$bucket && s3basekey==token$baseKey) {
      base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
    } else {
      base_s3_uri <- paste0('s3://', s3bucket, '/', s3basekey)
    }
    
    Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
               'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
               'AWS_SESSION_TOKEN'=token$sessionToken)
    
    unlink(downloadLocation, recursive = T, force = T)
    sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {downloadLocation} --exclude "*owner.txt*" --exclude "*archive*"')
    system(sync_cmd)
    
    file_paths <- list.files(downloadLocation, recursive = F, full.names = T)
    file_paths <- file_paths[grepl("dataset_", (file_paths), ignore.case = T)]
    
    df_list <- lapply(file_paths, function(file_path) {
      if (grepl("dataset_.*", file_path)) {
        if (grepl(".*fitbit.*intraday.*", file_path)) {
          arrow::open_dataset(file_path) %>% 
            dplyr::select(dplyr::all_of(c("ParticipantIdentifier", 
                                          "DateTime", 
                                          "DeepSleepSummaryBreathRate", 
                                          "RemSleepSummaryBreathRate", 
                                          "FullSleepSummaryBreathRate", 
                                          "LightSleepSummaryBreathRate"))) %>% 
            dplyr::collect()
        } else {
          arrow::open_dataset(file_path) %>% dplyr::collect()
        }
      } else {
        stop(paste("Unsupported file format for files in dataset:", file_path))
      }
    })
    
    names(df_list) <- 
      file_paths %>% 
      {paste(basename(.))} %>%
      {gsub("\\.(parquet|tsv|ndjson)$|(dataset_|-.*\\.snappy| )", "", .)}
    
    if (!is.null(dataset_name_filter)) {
      pattern <- paste(dataset_name_filter, collapse="|")
      df_list <- df_list[grepl(tolower(pattern), tolower(names(df_list))) & !grepl("manifest", tolower(names(df_list)))]
    }
    
  } else {
    stop(cat("Error: parameter 'method' must be one of 'synapse' or 'sts'"))
  }
  
  return(df_list)
}
