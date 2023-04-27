# This function takes a data frame and a vector of excluded concepts (strings) as input, and reshapes the data frame from wide to long format, keeping the columns whose names are in the "excluded concepts" vector as is, and reshaping only the remaining columns. The resulting data frame has a "concept" column containing the original column names (those that are not found in "excluded concepts"), and a "value" column containing the values of those columns, in a long format.
melt_df <- function(df, excluded_concepts) {
  approved_cols <- setdiff(names(df), excluded_concepts)
  df_melt <- reshape2::melt(df, 
                  id.vars = intersect(excluded_concepts, names(df)), 
                  measure.vars = approved_cols,
                  variable.name = "concept", 
                  value.name = "value")
  return(df_melt)
}
