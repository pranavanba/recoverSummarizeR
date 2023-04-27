# This function takes a named vector as input and returns a new named vector, where the names and values in the input vector are reversed.
reverse_str_pairs <- function(str_pairs) {
  reversed <- stats::setNames(names(str_pairs), str_pairs)
  reversed <- rev(reversed)
  return(reversed)
}
