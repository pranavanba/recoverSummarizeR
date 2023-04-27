# This function takes a list of data frames, a named vector of strings (concept replacements), and a data frame (concept map) as input, and returns a vector of excluded concepts (strings in concept_cd column) that are present in the input data frames but not in the approved summarized concepts in the concept map data frame.
diff_concepts <- function(df_list, concept_replacements, concept_map) {
  all_cols <- df_list %>%
    lapply(names) %>%
    unlist() %>%
    tibble::enframe() %>%
    dplyr::mutate(name = gsub("\\s\\d+|\\d", "", name))
  
  approved_concepts_summarized <- 
    concept_map$concept_cd %>%
    grep("^(?=.*summary)(?!.*trigger).+$", ., perl = T, value = T) %>% 
    stringr::str_extract("(?<=:)[^:]*$") %>% 
    stringr::str_replace_all(concept_replacements) %>% 
    unique()
  
  excluded_concepts <- 
    all_cols$value %>% 
    unique() %>% 
    setdiff(tolower(approved_concepts_summarized)) %>% 
    unique()
  
  return(excluded_concepts)
}
