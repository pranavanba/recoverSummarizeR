#' Execute recoverSummarizeR main pipeline
#'
#' @param ontologyFileID A Synapse ID for a CSV file stored in Synapse. For RECOVER, this file is the i2b2 concepts map.
#' @param parquetDirID A Synapse ID for a folder entity in Synapse where the data is stored. For RECOVER, this would be
#'   the folder housing the post-ETL parquet data.
#' @inheritParams syn_file_to_df
#' @inheritParams syn_dir_to_dflist
#' @inheritParams diff_concepts
#' @inheritParams store_in_syn
#' @inheritParams syn_parquet_dataset_to_dflist
#' @export
#' @examples
#' \dontrun{
#' synapser::synLogin()
#' ontologyFileID <- "syn12345678"
#' parquetDirID <- "syn87654321"
#' dataset_name_filter <- "fitbit"
#' concept_replacements <- c("mins" = "minutes",
#'                           "avghr" = "averageheartrate",
#'                           "spo2" = "spo2_",
#'                           "hrv" = "hrv_dailyrmssd",
#'                           "restinghr" = "restingheartrate",
#'                           "sleepbrth" = "sleepsummarybreath")
#' concept_filter_col <- "concept_cd"
#' synFolderID <- "syn18273645"
#' method <- "synapse"
#' s3bucket <- "my-project-bucket"
#' s3basekey <- "main/parquet/"
#' downloadLocation <- "./parquet"
#' 
#' summarize_pipeline(ontologyFileID = ontologyFileID,
#'                    parquetDirID = parquetDirID,
#'                    dataset_name_filter = dataset_name_filter,
#'                    concept_replacements = concept_replacements,
#'                    concept_filter_col = concept_filter_col,
#'                    synFolderID = synFolderID,
#'                    method = method,
#'                    s3bucket = s3bucket,
#'                    s3basekey = s3basekey,
#'                    downloadLocation = downloadLocation)
#' }
summarize_pipeline <- function(ontologyFileID, 
                               parquetDirID, 
                               dataset_name_filter, 
                               concept_replacements, 
                               concept_filter_col, 
                               synFolderID, 
                               method="synapse", 
                               s3bucket=NULL, 
                               s3basekey=NULL, 
                               downloadLocation) {
  
  cat("Running summarize_pipeline()...\n")
  
  concept_map <- syn_file_to_df(ontologyFileID, "concept_cd")
  cat("syn_file_to_df() completed.\n")
  
  df_list_original <- syn_parquet_dataset_to_dflist(parquetDirID, 
                                                    method, 
                                                    s3bucket, 
                                                    s3basekey, 
                                                    downloadLocation, 
                                                    dataset_name_filter)
  cat("syn_parquet_dataset_to_dflist() completed.\n")
  
  df_list_unified_tmp <- 
    unify_dfs(df_list_original) %>% 
    lapply(function(x) {
      names(x) <- tolower(names(x))
      return(x)})
  cat("unify_dfs() completed.\n")

  df_list <- 
    df_list_unified_tmp %>% 
    lapply(function(df) {
      names(df) <- gsub("value", "value_original", names(df))
      return(df)
    })
  
  concept_replacements_reversed <- vec_reverse(concept_replacements)
  cat("vec_reverse() completed.\n")
  
  excluded_concepts <- diff_concepts(df_list, concept_replacements, concept_map, concept_filter_col)
  cat("diff_concepts() completed.\n")
  
  df_list_melted_filtered <- 
    df_list %>% 
    lapply(melt_df, excluded_concepts) %>% 
    lapply(function(x) {
      x %>% 
        dplyr::select(if("participantidentifier" %in% colnames(x)) "participantidentifier",
                      dplyr::matches("(?<!_)date(?!_)", perl = T),
                      if("concept" %in% colnames(x)) "concept",
                      if("value" %in% colnames(x)) "value")}) %>% 
    {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
    lapply(tidyr::drop_na, "value")
  cat("melt_df() completed.\n")
  
  df_list_melted_filtered <- df_list_melted_filtered %>% convert_col_to_numeric()
  cat("convert_col_to_numeric() completed.\n")
  
  df_list_melted_filtered$fitbitintradaycombined <- 
    df_list_melted_filtered$fitbitintradaycombined %>% 
    dplyr::mutate(value = ifelse(value>=0, value, NA))
  
  df_summarized <- 
    df_list_melted_filtered %>% 
    lapply(function(df) {
      df %>%
        dplyr::rename(startdate = dplyr::any_of(c("date", "datetime"))) %>%
        dplyr::mutate(enddate = if (!("enddate" %in% names(.))) NA else enddate) %>% 
        dplyr::select(-dplyr::any_of(c("modifieddate", "inserteddate"))) %>% 
        dplyr::select(participantidentifier, startdate, enddate, concept, value)
    }) %>% 
    dplyr::bind_rows() %>% 
    dplyr::filter("concept" %in% colnames(.)) %>% 
    stat_summarize() %>% 
    dplyr::distinct()
  cat("stat_summarize() completed.\n")
  
  output_concepts <- 
    process_df(df_summarized, concept_map, concept_replacements_reversed) %>% 
    dplyr::mutate(nval_num = signif(nval_num, 9)) %>% 
    dplyr::arrange(concept) %>% 
    dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character)) %>% 
    replace(is.na(.), "<null>") %>% 
    dplyr::filter(nval_num != "<null>" | tval_char != "<null>")
  cat("process_df() completed.\n")
  
  cat("Writing output_concepts to csv file...\n")
  utils::write.csv(output_concepts, file = '~/output_concepts.csv', row.names = F)
  cat("write.csv() completed.\n")
  cat("Writing concept_map to csv file...\n")
  utils::write.csv(concept_map, file = '~/concepts_map.csv', row.names = F)
  cat("write.csv() completed.\n")
  
  store_in_syn(synFolderID, 
               '~/output_concepts.csv', 
               used_param = c(ontologyFileID, parquetDirID), 
               executed_param = as.character(
                                             paste("https://github.com/Sage-Bionetworks/recoverSummarizeR/releases/tag/", 
                                                   as.character(packageVersion("recoverSummarizeR")), 
                                                   sep = ""))
                                            )
  cat("store_in_syn() completed.\n")
  store_in_syn(synFolderID, '~/concepts_map.csv', used_param = ontologyFileID)
  cat("store_in_syn() completed.\n")
  
  cat("summarize_pipeline() completed.\n")
}
