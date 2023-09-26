#' Store Files in Synapse
#'
#' `store_in_syn()` creates a new Synapse Entity with any associated provenance, if provided, uploading any files in the
#' process.
#'
#' @param synFolderID A Synapse ID for a folder entity in Synapse.
#' @param filepath The path for the file to be uploaded to Synapse.
#' @param used_param The Synapse ID, URL, file name (if in the same working directory), or file path used to create the
#'   object (can also be a list of these).
#' @param executed_param The Synapse ID, URL, file name (if in the same working directory), or file path representing
#'   code executed to create the object (can also be a list of these).
#' @return A Synapse Entity.
#' @export
#'
#' @examples
#' \dontrun{
#' synFolderID <- "syn12345678"
#' ontologyFileID <- "syn87654321"
#' 
#' store_in_syn(synFolderID,
#'              'file1.csv',
#'              used_param = ontologyFileID,
#'              executed_param = "https://github.com/username/repo/blob/branch/script.R")
#'              
#' store_in_syn(synFolderID,
#'              'file2.csv',
#'              used_param = ontologyFileID)
#' }
store_in_syn <- function(synFolderID, filepath, used_param = NULL, executed_param = NULL) {
  cat("Running store_in_syn()...\n")
  
  fileObj <- synapser::File(path = filepath, parent = synFolderID, name = basename(filepath))
  if (!is.null(used_param) && !is.null(executed_param)) {
    fileObj <- synapser::synStore(fileObj, used = used_param, executed = executed_param)
  } else if (!is.null(used_param)) {
    fileObj <- synapser::synStore(fileObj, used = used_param)
  } else if (!is.null(executed_param)) {
    fileObj <- synapser::synStore(fileObj, executed = executed_param)
  } else {
    fileObj <- synapser::synStore(fileObj)
  }
  return(fileObj)
}
