test_that("function works as expected", {
  synapser::synLogin()
  
  expect_no_error(syn_file_to_df("syn51218416"))
  expect_no_error(syn_file_to_df("syn51218416", "Definition"))
  expect_no_error(syn_file_to_df("syn51218416", c("Definition", "c_name")))
})
