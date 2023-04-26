# This function takes a list of data frames, a string denoting the full or partial name of a data frame, and a column name as input, then converts all instances of that column in the data frames in the input list of data frames to numeric type except in the data frame(s) whose name(s) contain the string specified by the df_to_avoid argument.
convert_col_to_numeric <- function(df_list, df_to_avoid = "device", col_to_convert = "value") {
  for (i in seq_along(df_list)) {
    if (!grepl(df_to_avoid, names(df_list)[i])) {
      df_list[[i]][[col_to_convert]] <- sapply(df_list[[i]][[col_to_convert]], as.numeric)
    }
  }
  return(df_list)
}
