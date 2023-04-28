# This function takes a data frame, a concept map data frame, and named vector as inputs. First, the modifications made to the strings in the "concept" column in the data frame by diff_concepts() are reversed using the value pairs from concept_replacements_reversed. Then various columns are added to the input data frame, and the modified data frame is returned. An error is raised if a specific set of columns are not found in the input data frame.
process_df <- function(df, concept_map, concept_replacements_reversed, concept_map_concepts = concept_cd, concept_map_units = UNITS_CD) {
  if (any(grepl("summary", df$concept))) {
    df$concept %<>% 
      tolower() %>% 
      stringr::str_replace_all(concept_replacements_reversed)
  }
  
  if (all((c("participantidentifier", "startdate", "enddate", "concept", "value") %in% colnames(df)))) {
    if (all(c("valtype_cd", "nval_num", "tval_char") %in% colnames(df))) {
      df %<>% 
        dplyr::mutate(startdate = lubridate::as_date(startdate),
               enddate = lubridate::as_date(enddate)) %>%
        dplyr::left_join(dplyr::select(concept_map, concept_map_concepts, concept_map_units),
                  by = c("concept" = concept_map_concepts)) %>% 
        tidyr::drop_na(valtype_cd)
    } else {
      df %<>%
        dplyr::mutate(startdate = lubridate::as_date(startdate),
               enddate = lubridate::as_date(enddate)) %>%
        dplyr::mutate(valtype_cd = dplyr::case_when(class(value) == "numeric" ~ "N", 
                                      class(value) == "character" ~ "T")) %>%
        dplyr::mutate(nval_num = as.numeric(dplyr::case_when(valtype_cd == "N" ~ value)),
               tval_char = as.character(dplyr::case_when(valtype_cd == "T" ~ value))) %>%
        dplyr::select(-value) %>%
        dplyr::left_join(dplyr::select(concept_map, concept_map_concepts, concept_map_units),
                  by = c("concept" = concept_map_concepts)) %>% 
        tidyr::drop_na(valtype_cd)
    }
  } else {
    stop("Error: One or all of {participantidentifier, startdate, enddate, concept, value} columns not found")
  }
  colnames(df) <- tolower(colnames(df))
  return(df)
}
