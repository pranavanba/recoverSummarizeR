#'Reshape selected columns from wide to long format
#'
#'`melt_df()` reshapes specific columns of a given data frame from wide to long format. The "excluded concepts" vector
#'allows users to specify which columns should be excluded from this reshaping.
#'
#'@param df A data frame.
#'@param excluded_concepts A vector of column names to not melt.
#'
#'@return A data frame of the same type as `df` but different shape. The resulting data frame has two new columns:
#'  "concept," which contains the names of the melted columns, and "value," which contains the values of those melted
#'  columns. `melt_df()` does not modify the excluded columns and only reshapes the remaining columns.
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
#' melted_df <- melt_df(mtcars)
#' head(melted_df)
melt_df <- function(df, excluded_concepts = "") {
  if (!is.data.frame(df)) stop("df must be a data frame")
  if (!is.vector(excluded_concepts)) stop("excluded_concepts must be a vector")
  if (!is.character(excluded_concepts)) stop("excluded_concepts needs to be of type 'character'")
  
  approved_cols <- setdiff(names(df), excluded_concepts)
  df_melt <- reshape2::melt(df, 
                  id.vars = intersect(excluded_concepts, names(df)), 
                  measure.vars = approved_cols,
                  variable.name = "concept", 
                  value.name = "value")
  return(df_melt)
}
