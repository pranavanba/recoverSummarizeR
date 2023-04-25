# This function fetches the ontology file (concepts map) from Synapse based on the synID of the stored file
get_concept_map <- function(synID) {
  df <- 
    synGet(synID) %>% 
    {read.csv(.$path)} %>% 
    mutate(concept_cd = tolower(concept_cd))
  
  return(df)
}
