test_that("function works correctly", {
  synapser::synLogin()

  ontologyFileID <- "syn51320791"
  parquetDirID <- "syn50996868"
  dataset_name_filter <- "fitbit"
  concept_replacements <- c("mins" = "minutes",
                            "avghr" = "averageheartrate",
                            "spo2" = "spo2_",
                            "hrv" = "hrv_dailyrmssd",
                            "restinghr" = "restingheartrate",
                            "sleepbrth" = "sleepsummarybreath")
  concept_filter_col <- "concept_cd"
  synFolderID <- "syn51184127"

  summarize_pipeline(ontologyFileID = ontologyFileID,
           parquetDirID = parquetDirID,
           dataset_name_filter = dataset_name_filter,
           concept_replacements = concept_replacements,
           concept_filter_col = concept_filter_col,
           synFolderID = synFolderID)
  
  newest_output_concepts_version <- synapser::synGet('syn51277458')$versionLabel
  newest_concept_map_version <- synapser::synGet('syn51218416')$versionLabel
  
  pretest_output_concepts <- 
    synapser::synGet('syn51277458', version = 29) %>% 
    {read.csv(.$path)} %>% 
    dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character))
  
  pretest_concept_map <- 
    synapser::synGet('syn51218416', version = 29) %>% 
    {read.csv(.$path)}
  
  posttest_output_concepts <- 
    synapser::synGet('syn51277458', version = newest_output_concepts_version) %>% 
    {read.csv(.$path)} %>% 
    dplyr::mutate(dplyr::across(.cols = dplyr::everything(), .fns = as.character))
  
  posttest_concept_map <- 
    synapser::synGet('syn51218416', version = newest_concept_map_version) %>% 
    {read.csv(.$path)}
  
  expect_true(all.equal(pretest_output_concepts, posttest_output_concepts))
  expect_true(all.equal(pretest_concept_map, posttest_concept_map))
})
