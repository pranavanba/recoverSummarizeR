mainflow <- function(ontologyFileID, parquetDirID, concept_replacements, concept_filter_col, synFolderID) {
  concept_map <- get_concept_map(ontologyFileID)
  
  df_list_original <- synget_parquet_to_df(parquetDirID, "fitbit")
  
  df_list_unified_tmp <- 
    combine_duplicate_dfs(df_list_original) %>% 
    lapply(function(x) {
      names(x) <- tolower(names(x))
      return(x)})
  df_list <- 
    df_list_unified_tmp %>% 
    lapply(function(df) {
      names(df) <- gsub("value", "value_original", names(df))
      return(df)
    })
  
  concept_replacements_reversed <- reverse_str_pairs(concept_replacements)
  excluded_concepts <- diff_concepts(df_list, concept_replacements, concept_map, concept_filter_col) #= "concept_cd")
  
  df_list_melted_filtered <- 
    df_list %>% 
    lapply(melt_df, excluded_concepts) %>% 
    lapply(function(x) {
      x %>% 
        select(if("participantidentifier" %in% colnames(x)) "participantidentifier",
               matches("(?<!_)date(?!_)", perl = T),
               if("concept" %in% colnames(x)) "concept",
               if("value" %in% colnames(x)) "value")
    }) %>% 
    {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
    lapply(drop_na, "value")
  
  df_list_melted_filtered %<>% convert_col_to_numeric()
  
  df_summarized <- 
    df_list_melted_filtered %>% 
    {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
    lapply(stat_summarize) %>% 
    bind_rows() %>% 
    distinct()
  
  output_concepts <- 
    process_df(df_summarized, concept_map, concept_replacements_reversed) %>% 
    mutate(nval_num = signif(nval_num, 9)) %>% 
    arrange(concept) %>% 
    mutate(across(.fns = as.character)) %>% 
    replace(is.na(.), "<null>") %>% 
    filter(nval_num != "<null>" | tval_char != "<null>")
  
  write.csv(output_concepts, file = 'output_concepts.csv', row.names = F)
  write.csv(concept_map, file = 'concepts_map.csv', row.names = F)
  
  store_in_syn(synFolderID, 'output_concepts.csv', used_param = ontologyFileID, executed_param = "https://github.com/Sage-Bionetworks/recoverSummarizeR/tree/release")
  store_in_syn(synFolderID, 'concepts_map.csv', used_param = ontologyFileID)
}
