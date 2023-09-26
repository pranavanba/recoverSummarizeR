#' Reverse the order and pairings of a named vector
#'
#' `vec_reverse()` reverses both the order of a named vector and its name = value pairings.
#'
#' @param str_pairs A named vector.
#'
#' @return A named vector of the same type as `str_pairs`.
#' @export
#' @examples
#' # Create a named vector of strings
#' fruit_colors <- c(apple = "red", banana = "yellow", cherry = "red",
#'                   kiwi = "brown", lemon = "yellow")
#'
#' # Reverse the order of the named vector and its values
#' reversed_fruit_colors <- vec_reverse(fruit_colors)
#'
#' # Print the original and reversed named vectors
#' print(fruit_colors)
#' print(reversed_fruit_colors)
vec_reverse <- function(str_pairs) {
  cat("Running vec_reverse()...\n")
  
  if (!is.vector(str_pairs)) stop("str_pairs must be a vector")
  if (is.numeric(str_pairs)) stop("str_pairs is numeric and must be of type 'character'")
  
  reversed <- stats::setNames(names(str_pairs), str_pairs)
  reversed <- rev(reversed)
  return(reversed)
}
