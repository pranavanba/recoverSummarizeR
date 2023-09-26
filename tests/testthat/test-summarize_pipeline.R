test_that("function works correctly for synapse method", {
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
  method <- "synapse"
  downloadLocation <- "./parquet"

  summarize_pipeline(ontologyFileID = ontologyFileID,
                     parquetDirID = parquetDirID,
                     dataset_name_filter = dataset_name_filter,
                     concept_replacements = concept_replacements,
                     concept_filter_col = concept_filter_col,
                     synFolderID = synFolderID,
                     method = method,
                     downloadLocation = downloadLocation)
  
  newest_output_concepts_version <- synapser::synGet('syn51277458')$versionLabel
  newest_concept_map_version <- synapser::synGet('syn51218416')$versionLabel
  
  pretest_output_concepts <- 
    synapser::synGet('syn51277458', version = 53) %>% 
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

test_that("function works correctly for sts method", {
  synapser::synLogin()
  
  ontologyFileID <- "syn51320791"
  parquetDirID <- "syn52504331"
  dataset_name_filter <- "fitbit"
  concept_replacements <- c("mins" = "minutes",
                            "avghr" = "averageheartrate",
                            "spo2" = "spo2_",
                            "hrv" = "hrv_dailyrmssd",
                            "restinghr" = "restingheartrate",
                            "sleepbrth" = "sleepsummarybreath")
  concept_filter_col <- "concept_cd"
  synFolderID <- "syn51184127"
  method <- "sts"
  s3bucket <- "sc-237179673806-pp-rrscq25af2qlg-s3bucket-15qsg0yihrmlf"
  s3basekey <- "main/parquet"
  downloadLocation <- "./parquet"
  
  summarize_pipeline(ontologyFileID = ontologyFileID,
                     parquetDirID = parquetDirID,
                     dataset_name_filter = dataset_name_filter,
                     concept_replacements = concept_replacements,
                     concept_filter_col = concept_filter_col,
                     synFolderID = synFolderID,
                     method = method,
                     s3bucket = s3bucket,
                     s3basekey = s3basekey,
                     downloadLocation = downloadLocation)
  
  newest_output_concepts_version <- synapser::synGet('syn51277458')$versionLabel
  newest_concept_map_version <- synapser::synGet('syn51218416')$versionLabel
  
  pretest_output_concepts <- 
    synapser::synGet('syn51277458', version = 53) %>% 
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
