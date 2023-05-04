test_that("function works correctly", {
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
  mainflow(ontologyFileID = ontologyFileID, 
           parquetDirID = parquetDirID, 
           concept_replacements = concept_replacements, 
           concept_filter_col = concept_filter_col, 
           synFolderID = synFolderID)
  
  pretest_output_concepts <- 
    synapser::synGet('syn51277458', version = 29) %>% 
    {read.csv(.$path)} %>% 
    dplyr::mutate(dplyr::across(.fns = as.character))
  pretest_concept_map <- 
    synapser::synGet('syn51218416', version = 29) %>% 
    {read.csv(.$path)}
  
  posttest_output_concepts <- 
    synapser::synGet('syn51277458', version = 30) %>% 
    {read.csv(.$path)} %>% 
    dplyr::mutate(dplyr::across(.fns = as.character))
  posttest_concept_map <- 
    synapser::synGet('syn51218416', version = 30) %>% 
    {read.csv(.$path)}
  
  expect_true(all.equal(pretest_output_concepts, posttest_output_concepts))
  expect_true(all.equal(pretest_concept_map, posttest_concept_map))
})
