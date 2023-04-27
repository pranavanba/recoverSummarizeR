# This function fetches the ontology file (concepts map) from Synapse based on the synID of the stored file.
get_concept_map <- function(synID) {
  df <- 
    synapser::synGet(synID) %>% 
    {utils::read.csv(.$path)} %>% 
    dplyr::mutate(concept_cd = tolower(concept_cd))
  
  return(df)
}
