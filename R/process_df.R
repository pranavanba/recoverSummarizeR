# This function takes a data frame, a concept map data frame, and named vector as inputs. First, the modifications made to the strings in the "concept" column in the data frame by diff_concepts() are reversed using the value pairs from concept_replacements_reversed. Then various columns are added to the input data frame, and the modified data frame is returned. An error is raised if a specific set of columns are not found in the input data frame.
process_df <- function(df, concept_map, concept_replacements_reversed) {
  if (any(grepl("summary", df$concept))) {
    df$concept %<>% 
      tolower() %>% 
      str_replace_all(concept_replacements_reversed)
  }
  
  if (all((c("participantidentifier", "startdate", "enddate", "concept", "value") %in% colnames(df)))) {
    if (all(c("valtype_cd", "nval_num", "tval_char") %in% colnames(df))) {
      df %<>% 
        mutate(startdate = as_date(startdate),
               enddate = as_date(enddate)) %>%
        left_join(select(concept_map, concept_cd, UNITS_CD),
                  by = c("concept" = "concept_cd")) %>% 
        drop_na(valtype_cd)
    } else {
      df %<>%
        mutate(startdate = as_date(startdate),
               enddate = as_date(enddate)) %>%
        mutate(valtype_cd = case_when(class(value) == "numeric" ~ "N", 
                                      class(value) == "character" ~ "T")) %>%
        mutate(nval_num = as.numeric(case_when(valtype_cd == "N" ~ value)),
               tval_char = as.character(case_when(valtype_cd == "T" ~ value))) %>%
        select(-value) %>%
        left_join(select(concept_map, concept_cd, UNITS_CD),
                  by = c("concept" = "concept_cd")) %>% 
        drop_na(valtype_cd)
    }
  } else {
    stop("Error: One or all of {participantidentifier, startdate, enddate, concept, value} columns not found")
  }
  colnames(df) <- tolower(colnames(df))
  return(df)
}
