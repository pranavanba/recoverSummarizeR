#' Process a data frame with concept mappings
#'
#' @description `process_df()` takes a primary data frame (`df`), a secondary data frame (`concept_map`), and named
#' vector as inputs. First, the modifications made to the strings in the `concept_filter_col` column in `concept_map` by
#' passing `concept_replacements` to \code{\link{diff_concepts}} are reversed using the `name = value` pairs from
#' `concept_replacements_reversed`. Then, various columns are added to `df`, and the modified data frame is returned. An
#' error is raised if a specific set of columns are not found in `df`.
#'
#' @param df A data frame with `participantidentifier`, `startdate`, `enddate`, `concept`, and `value` columns.
#' @param concept_map A data frame (for i2b2/RECOVER purposes, this data frame is the ontology file).
#' @param concept_replacements_reversed A lowercase named vector that can be created using
#'   \code{\link{reverse_str_pairs}}; names must be valid values of `df$concept`, and values must contain the original
#'   names from the `concept_replacements` named vector used for \code{\link{diff_concepts}}
#' @param concept_map_concepts The name of the column in `concept_map` that is the equivalent of `df$concept`.
#' @param concept_map_units The name of the column in `concept_map` containing values/info for the units of each value
#'   in `df$concept`.
#'
#' @return A data frame with a different shape than that of `df`.
#' @export
#'
#' @examples
#' # Create a sample data frame
#' df <- data.frame(participantidentifier = c(1, 2, 3, 4),
#'                  startdate = c("2022-01-01", "2022-01-01", "2022-01-02", "2022-01-03"),
#'                  enddate = c("2022-01-03", "2022-01-02", "2022-01-03", "2022-01-04"),
#'                  concept = c("summary:BLOOD_PRESSURE_SYSTOLIC", "summary:BLOOD_PRESSURE_DIASTOLIC",
#'                              "summary:HEART_RATE", "summary:RESPIRATORY_RATE"),
#'                  value = c(120, 80, 70, 20))
#' print(df)
#'
#' # Create a sample `concept_map` data frame
#' concept_map <- data.frame(concept_cd = c("summary:BP_SYSTOLIC", "summary:BP_DIASTOLIC",
#'                                          "summary:HR", "summary:RR"),
#'                           valtype_cd = c("N", "N", "N", "N"),
#'                           UNITS_CD = c("mmHg", "mmHg", "beats/min", "breaths/min"))
#' print(concept_map)
#'
#' # Assume `concept_replacements` already exists
#' concept_replacements <- c("bp" = "blood_pressure", "hr" = "heart_rate", "rr" = "respiratory_rate")
#'
#' # Create a reversed named vector of concept replacements
#' concept_replacements_reversed <- reverse_str_pairs(concept_replacements)
#' print(concept_replacements_reversed)
#'
#' # Process the data frame
#' df_processed <- process_df(df, concept_map, concept_replacements_reversed, "concept_cd", "UNITS_CD")
#' head(df_processed)
process_df <- function(df, concept_map, concept_replacements_reversed, concept_map_concepts = "concept_cd", concept_map_units = "UNITS_CD") {
  if (any(grepl("summary", df$concept))) {
    df$concept <- 
      df$concept %>% 
      tolower() %>% 
      stringr::str_replace_all(concept_replacements_reversed)
  }
  
  concept_map[[concept_map_concepts]] <- concept_map[[concept_map_concepts]] %>% tolower()
  
  if (all((c("participantidentifier", "startdate", "enddate", "concept", "value") %in% colnames(df)))) {
    if (all(c("valtype_cd", "nval_num", "tval_char") %in% colnames(df))) {
      df <- 
        df %>%
        dplyr::mutate(startdate = lubridate::as_date(startdate),
               enddate = lubridate::as_date(enddate)) %>%
        dplyr::left_join(dplyr::select(concept_map, concept_map_concepts, concept_map_units),
                  by = c("concept" = concept_map_concepts)) %>% 
        tidyr::drop_na(valtype_cd)
    } else {
      df <- 
        df %>%
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
