#' Download Parquet Datasets from Synapse or an STS-Enabled Location
#'
#' `syn_parquet_dataset_to_dflist()` downloads objects from a Synapse directory or syncs from an STS-enabled location, such as an S3 bucket, and maintains the original directory
#' structure, reads the parquet datasets to data frames, and stores the data frames in a list.
#'
#' @param synDirID A Synapse ID for a folder entity in Synapse.
#' @param method Either 'synapse' or 'sts' to specify the method to use in getting the parquet datasets. 'synapse' will get files directly from a synapse project or folder using the synapse client, while 'sts' will use sts-token access to get objects from an sts-enabled storage location, such as an S3 bucket.
#' @param s3bucket The name of the S3 bucket to access when `method='sts'`.
#' @param s3basekey The base key of the S3 bucket to access when `method='sts'`.
#' @param downloadLocation The location to download files to.
#' @param dataset_name_filter A string used to filter the list of data frames by including only the data frames whose
#'   names contain the specified string.
#'
#' @return A list of data frames. The list is filtered to include only the data frames whose names contain the string
#'   specified by `dataset_name_filter`.
#' @export
#' @examples
#' \dontrun{
#' df_list <- syn_parquet_dataset_to_dflist("syn12345678", 
#'                                          "sts", 
#'                                          "s3://my-bucket/", 
#'                                          "main/folder/", 
#'                                          "./parquet", 
#'                                          "fitbit")
#' # df_list will use the `sts` method to get objects in synapse at 
#'   the synDirID and the output will contain only data frames for 
#'   parquet datasets whose names contain the string "fitbit"
#' }
syn_parquet_dataset_to_dflist <- function(synDirID, method="synapse", s3bucket=NULL, s3basekey=NULL, downloadLocation, dataset_name_filter=NULL) {
  cat("Running syn_parquet_dataset_to_df_list()...\n")
  
  if (method=="synapse") {
    unlink(downloadLocation, recursive = T, force = T)
    system(glue::glue("synapse get -r {synDirID} --manifest suppress --downloadLocation {downloadLocation}"))
    
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
    inclusions <- paste0("--include \"*",dataset_name_filter,"*\"", collapse = " ")
    sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {downloadLocation} --exclude "*" {inclusions}')
    system(sync_cmd)
    
  } else {
    stop(cat("Error: parameter 'method' must be one of 'synapse' or 'sts'"))
  }
  
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
          dplyr::mutate(
            drop_row = ifelse(
              test = 
                is.na(DeepSleepSummaryBreathRate) & 
                is.na(RemSleepSummaryBreathRate) & 
                is.na(FullSleepSummaryBreathRate) & 
                is.na(LightSleepSummaryBreathRate),
              yes = "drop", 
              no = "keep")) %>% 
          dplyr::filter(drop_row=="keep") %>% 
          dplyr::select(-(dplyr::any_of(c("drop_row")))) %>% 
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
  
  return(df_list)
}
