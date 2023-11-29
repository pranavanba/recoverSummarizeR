#' Convert specified column to numeric type in selected data frames
#'
#' @description `convert_col_to_numeric()` converts a specified column to numeric type in all data frames in a given
#'   list, except in the data frames whose names contain a string specified by the user.
#'
#' @param df_list A list of data frames.
#' @param df_to_avoid A string denoting the full or partial name of a data frame in `df_list`.
#' @param col_to_convert The name of a column to convert to numeric type.
#'
#' @return A list of data frames with modified column types and is the same size as `df_list`.
#' @export
#'
#' @examples
#' # Example 1: Convert "value" column in all data frames of list `my_dfs` to numeric type
#' my_dfs <- list(data.frame(value = c("1", "2", "3")),
#'                data.frame(value = c("4", "5", "6")),
#'                data.frame(value = c("7", "8", "9")))
#'
#' names(my_dfs) <- c("1", "2", "3")
#'
#' my_dfs <- convert_col_to_numeric(my_dfs)
#'
#' # Example 2: Convert "my_col" column in all data frames of
#' # my_list to numeric type, except in "exclude_df"
#' my_list <- list(df1 = data.frame(my_col = c("1", "2", "3")),
#'                 df2 = data.frame(my_col = c("4", "5", "6")),
#'                 exclude_df = data.frame(my_col = c("7", "8", "9")))
#'
#' my_list <- convert_col_to_numeric(my_list, "exclude", "my_col")
convert_col_to_numeric <- function(df_list, df_to_avoid = "device", col_to_convert = "value") {
  cat("Running convert_col_to_numeric()...\n")
  
  if (is.data.frame(df_list))stop("df_list must be a list of data frames, not a single data frame")
  if (is.character(df_list)) stop("df_list must be a list of data frames, not a character")
  if (is.numeric(df_list)) stop("df_list must be a list of data frames, not numeric")
  if (is.logical(df_list)) stop("df_list must be a list of data frames, not a logical object")
  if (length(df_list) < 2) stop("df_list must have more than 1 element")
  if (!any(sapply(df_list, function(x) col_to_convert %in% names(x)))) stop("The value of col_to_convert is not found in the column names of any data frames in df_list")
  
  for (i in seq_along(df_list)) {
    if (!grepl(df_to_avoid, names(df_list)[i])) {
      if (col_to_convert %in% names(df_list[[i]])) {
        df_list[[i]][[col_to_convert]] <- sapply(df_list[[i]][[col_to_convert]], as.numeric)
      }
    }
  }
  return(df_list)
}
