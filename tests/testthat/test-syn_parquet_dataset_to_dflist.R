test_that("function works", {
  synDirIDsyn <- "syn50996868"
  synDirIDsts <- "syn52504331"
  s3bucket <- "sc-237179673806-pp-rrscq25af2qlg-s3bucket-15qsg0yihrmlf"
  s3basekey <- "main/parquet"
  downloadLocation <- "./parquet"
  dataset_name_filter <- "fitbit"
  
  expect_no_error(
    df_list_syn <- 
      syn_parquet_dataset_to_dflist(synDirIDsyn, 
                                    method = "synapse", 
                                    s3bucket, 
                                    s3basekey, 
                                    downloadLocation,
                                    dataset_name_filter))
  
  expect_no_error(
    df_list_sts <- 
      syn_parquet_dataset_to_dflist(synDirIDsts, 
                                    method = "sts", 
                                    s3bucket, 
                                    s3basekey, 
                                    downloadLocation,
                                    dataset_name_filter))
})
