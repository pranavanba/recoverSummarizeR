#'Melt specific columns of a data frame
#'
#'@description `melt_df()` takes a data frame and a vector as input, and reshapes only the specified columns of the data
#'frame from wide to long format. While keeping the columns whose names are in the "excluded concepts" vector untouched,
#'`melt_df()` reshapes only the remaining columns of `df`.
#'
#'The resulting data frame has a "concept" column containing the melted column names (those that are not found in
#'`excluded concepts`), and a "value" column containing the values of those melted columns.
#'
#'@param df A data frame.
#'@param excluded_concepts A vector of column names to not melt.
#'
#'@return A data frame of the same type as `df` but different shape.
#'@export
#'
#' @examples
#' #' # Example 1: Melt all columns except "Species" of the "iris" dataset
#' head(iris)
#' melted_df <- melt_df(iris, c("Species"))
#' head(melted_df)
#'
#' # Example 2: Melt columns "col1" and "col2" of a custom data frame
#' df <- data.frame(col1 = c(1,2,3), col2 = c(4,5,6), col3 = c(7,8,9), col4 = c(10, 11, 12))
#' head(df)
#' melted_df <- melt_df(df, c("col3", "col4"))
#' head(melted_df)
#'
#' # Example 3: Melt all columns of the "mtcars" dataset
#' head(mtcars)
#' melted_df <- melt_df(mtcars, c())
#' head(melted_df)
melt_df <- function(df, excluded_concepts) {
  if (!is.data.frame(df)) stop("df must be a data frame")
  if (!is.vector(excluded_concepts)) stop("excluded_concepts must be a vector")
  
  approved_cols <- setdiff(names(df), excluded_concepts)
  df_melt <- reshape2::melt(df, 
                  id.vars = intersect(excluded_concepts, names(df)), 
                  measure.vars = approved_cols,
                  variable.name = "concept", 
                  value.name = "value")
  return(df_melt)
}
