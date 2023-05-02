#' Fetch Synapse CSV file and store in data frame
#'
#' `get_concept_map()` fetches the concept map (ontology) CSV file from Synapse based on the synID of the stored file,
#' then reads the file to a data frame.
#'
#' @param synID A Synapse ID for a CSV file in Synapse.
#' @returns A data frame containing a representation of the data in the file. The values in the "concept_cd" column are
#'   converted to lowercase.
#' @export
#' @examples
#' \dontrun{
#' df <- get_concept_map("syn12345678")
#' }
#' 
get_concept_map <- function(synID) {
  df <- 
    synapser::synGet(synID) %>% 
    {utils::read.csv(.$path)} %>% 
    dplyr::mutate(concept_cd = tolower(concept_cd))
  
  return(df)
}
