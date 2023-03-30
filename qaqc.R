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

