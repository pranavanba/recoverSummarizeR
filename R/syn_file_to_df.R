#' Fetch a Synapse file and store in data frame
#'
#' `syn_file_to_df()` fetches a file from Synapse based on the synID of the stored file, then reads the file to a data
#' frame, with the option to lowercase a specific column. For i2b2/RECOVER, `syn_file_to_df()` can be used to fetch the
#' concept map (ontology) file from Synapse then read it into a data frame.
#'
#' @param synID A Synapse ID for a file in Synapse.
#' @param cols Names of one or more columns to convert to lowercase.
#'
#' @returns A data frame containing a representation of the data in the file. The values in the columns specified by
#'   `cols` are converted to lowercase. For i2b2/RECOVER, the default has been set to `concept_cd`.
#' @export
#' @examples
#' \dontrun{
#' df <- syn_file_to_df("syn12345678")
#' }
#' 
syn_file_to_df <- function(synID, cols = NULL) {
  df <- 
    synapser::synGet(synID) %>% 
    {
      if (grepl(".parquet$", .$path)) {
        arrow::read_parquet(.$path)
      } else if (grepl(".tsv$", .$path)) {
        readr::read_tsv(.$path, show_col_types = F)
      } else if (grepl(".ndjson$", .$path)) {
        ndjson::stream_in(.$path, cls = "tbl")
      } else if (grepl(".csv$", .$path)) {
        utils::read.csv(.$path)
      } else {
        stop(paste("Unsupported file format for", .$path))
      }
    } %>% 
    dplyr::mutate(dplyr::across(dplyr::all_of(cols), tolower))
  
  return(df)
}
