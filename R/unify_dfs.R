#' Combine data frames with identical names
#'
#' `unify_dfs()` combines data frames with the same name in a list into a unified data frame. This function
#' is especially useful when working with large data sets, where duplicate data frames can be generated from partitioned
#' files; i.e., partitioned files that are read into data frames can have identical names, so `unify_dfs()`
#' essentially de-partitions those data.
#'
#' @param df_list A list of data frames.
#' @returns A list of data frames with unique names. The output list will be shorter than the input list.
#'
#'   The length of the output list can be determined by `n - d + u`, where `n` is the length of the original list, `d`
#'   is the number of duplicate names in the list, and `u` is the number of unique duplicate names in the list.
#' @examples
#' # Create some example data frames
#' df1 <- data.frame(x = 1:3, y = letters[1:3])
#' df2 <- data.frame(x = 4:6, y = letters[4:6])
#' df3 <- data.frame(x = 7:9, y = letters[7:9])
#' df4 <- data.frame(x = 10:12, y = letters[10:12])
#' df_list <- list(df1, df2, df1, df3, df4, df2)
#' names(df_list) <- c("df1", "df2", "df1", "df3", "df4", "df2")
#'
#' # Combine the data frames
#' combined_df_list <- unify_dfs(df_list)
#' head(combined_df_list)
#' @export
unify_dfs <- function(df_list) {
  if (is.data.frame(df_list))stop("df_list must be a list of data frames, not a single data frame")
  if (is.character(df_list)) stop("df_list must be a list of data frames, not a character")
  if (is.numeric(df_list)) stop("df_list must be a list of data frames, not numeric")
  if (is.logical(df_list)) stop("df_list must be a list of data frames, not a logical object")
  if (length(df_list) < 2) stop("df_list must have more than 1 element")
  
  df_names <- unique(names(df_list))
  
  for (i in seq_along(df_names)) {
    df_name <- df_names[i]
    df_matches <- grepl(paste0("^", df_name, "$"), names(df_list))
    if (sum(df_matches) > 1) {
      df_combined <- do.call(dplyr::bind_rows, df_list[df_matches])
      df_list <- c(df_list[!df_matches], list(df_combined))
      names(df_list)[length(df_list)] <- df_name
    }
  }
  return(df_list)
}
