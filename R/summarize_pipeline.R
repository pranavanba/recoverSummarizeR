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
  
  concept_map <- syn_file_to_df(ontologyFileID, "concept_cd")
  
  df_list_original <- syn_parquet_dataset_to_dflist(parquetDirID, 
                                                    method, 
                                                    s3bucket, 
                                                    s3basekey, 
                                                    downloadLocation, 
                                                    dataset_name_filter)
  
  df_list_unified_tmp <- 
    unify_dfs(df_list_original) %>% 
    lapply(function(x) {
      names(x) <- tolower(names(x))
      return(x)})
  
  df_list <- 
    df_list_unified_tmp %>% 
    lapply(function(df) {
      names(df) <- gsub("value", "value_original", names(df))
      return(df)
    })
  
  concept_replacements_reversed <- vec_reverse(concept_replacements)
  
  excluded_concepts <- diff_concepts(df_list, concept_replacements, concept_map, concept_filter_col)
  
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
  
  df_list_melted_filtered <- df_list_melted_filtered %>% convert_col_to_numeric()
  
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
  
  output_concepts <- 
    process_df(df_summarized, concept_map, concept_replacements_reversed) %>% 
    dplyr::mutate(nval_num = signif(nval_num, 9)) %>% 
    dplyr::arrange(concept) %>% 
    dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character)) %>% 
    replace(is.na(.), "<null>") %>% 
    dplyr::filter(nval_num != "<null>" | tval_char != "<null>")
  
  utils::write.csv(output_concepts, file = '~/output_concepts.csv', row.names = F)
  utils::write.csv(concept_map, file = '~/concepts_map.csv', row.names = F)
  
  store_in_syn(synFolderID, '~/output_concepts.csv', used_param = c(ontologyFileID, parquetDirID), executed_param = "https://github.com/Sage-Bionetworks/recoverSummarizeR")
  store_in_syn(synFolderID, '~/concepts_map.csv', used_param = ontologyFileID)
}
