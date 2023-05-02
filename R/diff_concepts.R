#' Return the set difference of column names between data frames
#'
#' `diff_concepts()` takes a list of data frames and a secondary data frame (`concept_map`) as input, along with a named
#' vector and the name of a column in `concept_map`. First, `diff_concepts()` replaces various strings in
#' `concept_filter_col` with the named values in `concept_replacements`. Then, it returns a vector of column names that
#' are present in the data frames of `df_list` and not present in the values of the specified column
#' (`concept_filter_col`) in `concept_map`.
#'
#' @param df_list A list of data frames.
#' @param concept_replacements A named vector (names must be valid values of `concept_filter_col` column of
#'   `concept_map` data frame).
#' @param concept_map A data frame (for i2b2/RECOVER purposes, this data frame is the ontology file).
#' @param concept_filter_col The column of `concept_map` that contains "approved concepts" (column names of data frames
#'   in `df_list` that are not to be excluded).
#'
#' @return A vector of column names from the data frames in `df_list` that are not present in the values of the
#'   `concept_filter_col` column of `concept_map`.
#' @export
#'
#' @examples
#' # Create a sample data frame
#' first1 <- data.frame(col_a = 1:5, col_b = letters[1:5], col_c = TRUE)
#'
#' # Create another sample data frame
#' second2 <- data.frame(col_b = letters[6:10], col_d = 6:10, col_e = FALSE)
#' 
#' # Create a list of the data frames
#' df_list <- list(first1, second2)
#' 
#' # Set the names of the data frames in `df_list`
#' names(df_list) <- c("first1", "second2")
#'
#' # Create a `concept_map` data frame
#' concept_map <- data.frame(concept = c("summary:a", "summary:b", "trigger:c"))
#'
#' # Define the concept replacements
#' concept_replacements <- c("a" = "col_a", "b" = "col_b")
#'
#' # Get the set difference of column names between data frames
#' diff_concepts(df_list = df_list,
#'               concept_replacements = concept_replacements,
#'               concept_map = concept_map,
#'               concept_filter_col = "concept")
#' # Should return: "col_c" "col_d" "col_e"
diff_concepts <- function(df_list, concept_replacements, concept_map, concept_filter_col) {
  all_cols <- df_list %>%
    lapply(names) %>%
    unlist() %>%
    tibble::enframe() %>%
    dplyr::mutate(name = gsub("\\s\\d+|\\d", "", name))
  
  approved_concepts_summarized <- 
    concept_map[[concept_filter_col]] %>%
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
