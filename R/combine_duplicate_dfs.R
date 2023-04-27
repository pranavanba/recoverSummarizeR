# This function takes a list of data frames as input and combines any data frames with identical names into a single data frame. Partitioned files read into data frames will have identical names.
combine_duplicate_dfs <- function(df_list) {
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
