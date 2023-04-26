# This function takes the synID of a folder in Synapse, a local file's path, and synID/URL/filename/filepaths for data/code used and executed as inputs, and then stores the local file in the respective folder in Synapse with the associated used/executed provenance if provided.
store_in_syn <- function(synFolderID, filepath, used_param = NULL, executed_param = NULL) {
  fileObj <- synapser::File(path = filepath, parent = synFolderID, name = filepath)
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
