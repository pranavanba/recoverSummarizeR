# Find common columns to merge on ---------------------------------------------------------------------------------

# Common cols between fitbit sleep log files
identical(
  (df_list$fitbitsleeplogs$LogId %>% unique() %>% sort()), 
  (df_list$fitbitsleeplogs_sleeplogdetails$LogId %>% unique() %>% sort())
)

# Check overlap of concepts ---------------------------------------------------------------------------------------

approved_concepts <- tibble(name = "concept_map", value = str_extract(concept_map$concept_cd, "(?<=:)[^:]+$") %>% unique())
approved_concepts$value %<>% 
  gsub("Mins", "Minutes", .) %>% 
  gsub("AvgHR", "AverageHeartRate", .) %>% 
  gsub("SpO2", "SpO2_", .) %>% 
  gsub("Brth", "Breath", .) %>% 
  gsub("HrvD", "Hrv_D", .)
approved_concepts$value %<>% {gsub("RestingHR$", "RestingHeartRate", ., perl = T)}

new_df_list_concepts <- lapply(new_df_list, function(x) x$concept %>% unique()) %>% enframe() %>% unnest(cols = value)
new_df_list_concepts$included <- new_df_list_concepts$value %in% approved_concepts$value
new_df_list_concepts$value[new_df_list_concepts$name == "enrolledparticipants"] <- 
  str_extract(new_df_list_concepts$value[new_df_list_concepts$name == "enrolledparticipants"], "(?<=:)[^:]+$")

tmp <- synBuildTable("concepts_overlap", "syn43435581", new_df_list_concepts)
tmp <- synStore(tmp)
rm(tmp)

# Check wide to long format conversion ----------------------------------------------------------------------------

# Check that columns in data frames are not found in "concepts" list (which would indicate that not all concepts from
# original data sets were melted from wide to long format)

if ((T %in% lapply(lapply(new_df_list, function(x) names(x) %in% concepts), function(x) T %in% x))==T) {
  break
}

new_factors_summarized <- lapply(new_df_list_summarized, function(x) x[["concept"]] %>% unique())
new_factors_non_summarized <- lapply(new_df_list_non_summarized, function(x) x[["concept"]] %>% unique())


# Check that data frames include only approved concept variables --------------------------------------------------

# new_df_list$fitbitactivitylogs %>% 
#   # filter(concept %in% approved_concepts) %>% 
#   dim()

check_df_dims <- function(list_of_dfs, approved_concepts) {
  original_df_dims <- lapply(list_of_dfs, dim)
  
  filtered_df_dims <- lapply(list_of_dfs, function(x) {
    if ("concept" %in% colnames(x)) {
      x %>% 
        filter(concept %in% approved_concepts) %>% 
        dim()
    } else {
      dim(x)
    }
  })
  
  # Compare the dimensions of the original and filtered data frames
  for (i in seq_along(list_of_dfs)) {
    if (identical(original_df_dims[[i]], filtered_df_dims[[i]])) {
      message("Dimensions of original and filtered data frame ", i, " match.")
    } else {
      message("Dimensions of original and filtered data frame ", i, " do not match.")
    }
  }
}

check_df_dims(list_of_dfs = new_df_list, approved_concepts = approved_concepts)


# Check that concept codes are under 51 characters ----------------------------------------------------------------

if ((output_concepts$concept %>% nchar() %>% max()) > 50) {
  message("Some concept codes are longer than 50 characters in length")
}


# Check for empty value rows --------------------------------------------------------------------------------------

if (any(is.na(output_concepts$valtype_cd) | output_concepts$valtype_cd == "")) {
  message("There are values in the output file that are blank, i.e., there are concepts with NA or an empty string as the value")
}

