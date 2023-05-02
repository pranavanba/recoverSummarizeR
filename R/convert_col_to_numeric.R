#' Convert a specific column of specific data frames to numeric type
#'
#' @description `convert_col_to_numeric()` takes a list of data frames, a string denoting the full or partial name of a
#'   data frame, and a column name as input, then converts all instances of the specified column (`col_to_convert`) in
#'   the data frames in `df_list` to numeric type, except in the data frames whose names contain the string specified by
#'   `df_to_avoid`.
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
#' class(my_dfs[[1]]$value) # should return "character"
#'
#' my_dfs <- convert_col_to_numeric(my_dfs)
#' class(my_dfs[[1]]$value) # should return "numeric"
#'
#' # Example 2: Convert "my_col" column in all data frames of 
#' # my_list to numeric type, except in "exclude_df"
#' my_list <- list(df1 = data.frame(my_col = c("1", "2", "3")),
#'                 df2 = data.frame(my_col = c("4", "5", "6")),
#'                 exclude_df = data.frame(my_col = c("7", "8", "9")))
#'
#' class(my_list[[1]]$my_col) # should return "character"
#' class(my_list[[1]]$my_col) # should return "character"
#' class(my_list[[3]]$my_col) # should return "character"
#'
#' my_list <- convert_col_to_numeric(my_list, "exclude", "my_col")
#' class(my_list[[1]]$my_col) # should return "numeric"
#' class(my_list[[1]]$my_col) # should return "numeric"
#' class(my_list[[3]]$my_col) # should return "character"
convert_col_to_numeric <- function(df_list, df_to_avoid = "device", col_to_convert = "value") {
  for (i in seq_along(df_list)) {
    if (!grepl(df_to_avoid, names(df_list)[i])) {
      df_list[[i]][[col_to_convert]] <- sapply(df_list[[i]][[col_to_convert]], as.numeric)
    }
  }
  return(df_list)
}
