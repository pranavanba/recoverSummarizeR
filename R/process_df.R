#' Modify data frame using i2b2/RECOVER concept mappings
#'
#' @description `process_df()`  is a RECOVER-specific function which processes and adds specific columns to a data
#'   frame. An error is raised if any of the main RECOVER columns (`participantidentifier`, `startdate`, `enddate`,
#'   `concept`, `value`) are not found in the input data frame.
#'
#' @param df A data frame with `participantidentifier`, `startdate`, `enddate`, `concept`, and `value` columns.
#' @param concept_map A data frame (for i2b2/RECOVER purposes, this data frame is created from the concepts ontology
#'   file).
#' @param concept_replacements_reversed A lowercase named vector that can be created using
#'   \code{\link{reverse_str_pairs}}; names must be valid values of `df$concept`, and values must contain the original
#'   names from the `concept_replacements` named vector used for \code{\link{diff_concepts}}
#' @param concept_map_concepts The name of the column in `concept_map` that is the equivalent of `df$concept`.
#' @param concept_map_units The name of the column in `concept_map` that contains descriptions of the units for values
#'   in `df$concept`.
#'
#' @return A data frame with a different shape than that of `df`. The output data frame will have new columns:
#' \itemize{
#'    \item \strong{`valtype_cd`}: Either `N` or `T` depending on whether `class(value)` is `numeric` or `character`
#'    \item \strong{`nval_num`}: Populated with all numeric values (where `valtype_cd` is `N`)
#'    \item \strong{`tval_char`}: Populated with all character values (where `valtype_cd` is `T`)
#'    \item \strong{`UNITS_CD`}: The values of `concept_map$concept_map_units` joined by `df$concept` and `concept_map$concept_map_concepts`
#' }
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
process_df <- function(df, concept_map, concept_replacements_reversed = NULL, concept_map_concepts = "concept_cd", concept_map_units = "UNITS_CD") {
  if (!is.data.frame(df)) stop("df must be a data frame")
  if (!is.data.frame(concept_map)) stop("concept_map must be a data frame")
  if (!is.null(concept_replacements_reversed)) {
    if (!is.vector(concept_replacements_reversed)) stop("concept_replacements_reversed must be a vector")
  }
  if (!(concept_map_concepts %in% names(concept_map))) stop("concept_map_concepts must be the name of a column in concept_map")
  if (!is.character(concept_map_concepts)) stop("concept_map_concepts must be of type 'character'")
  if (!(concept_map_units %in% names(concept_map))) stop("concept_map_units must be the name of a column in concept_map")
  if (!is.character(concept_map_units)) stop("concept_map_units must be of type 'character'")

  if (any(grepl("summary", df$concept))) {
    df$concept <- 
      df$concept %>% 
      tolower() %>%
      {if (!is.null(concept_replacements_reversed)) {
          stringr::str_replace_all(., concept_replacements_reversed) 
        } else {
          .
        }
      }
  }
  
  concept_map[[concept_map_concepts]] <- concept_map[[concept_map_concepts]] %>% tolower()
  
  if (all((c("participantidentifier", "startdate", "enddate", "concept", "value") %in% colnames(df)))) {
    if (all(c("valtype_cd", "nval_num", "tval_char") %in% colnames(df))) {
      df <- 
        df %>%
        dplyr::mutate(startdate = lubridate::as_date(startdate),
               enddate = lubridate::as_date(enddate)) %>%
        dplyr::left_join(dplyr::select(concept_map, dplyr::all_of(c(concept_map_concepts, concept_map_units))),
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
        dplyr::select(dplyr::setdiff(names(.), "value")) %>%
        dplyr::left_join(dplyr::select(concept_map, dplyr::all_of(c(concept_map_concepts, concept_map_units))),
                  by = c("concept" = concept_map_concepts)) %>% 
        tidyr::drop_na(valtype_cd)
    }
  } else {
    stop("Error: One or all of {participantidentifier, startdate, enddate, concept, value} columns not found")
  }
  colnames(df) <- tolower(colnames(df))
  return(df)
}
