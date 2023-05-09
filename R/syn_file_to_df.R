#' Fetch Synapse CSV file and store in data frame
#'
#' `syn_file_to_df()` fetches the concept map (ontology) CSV file from Synapse based on the synID of the stored file,
#' then reads the file to a data frame.
#'
#' @param synID A Synapse ID for a CSV file in Synapse.
#' @returns A data frame containing a representation of the data in the file. The values in the "concept_cd" column are
#'   converted to lowercase.
#' @export
#' @examples
#' \dontrun{
#' df <- syn_file_to_df("syn12345678")
#' }
#' 
syn_file_to_df <- function(synID) {
  # TODO: update to allow specifying which column is 'concept_cd' and maybe rename function to better align with what the
  # function does (then update description and title as needed)
  df <- 
    synapser::synGet(synID) %>% 
    {utils::read.csv(.$path)} %>% 
    dplyr::mutate(concept_cd = tolower(concept_cd))
  
  return(df)
}
