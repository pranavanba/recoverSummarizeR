test_that("function works as expected", {
  synapser::synLogin()
  
  expect_no_error(syn_dir_to_dflist("syn50996868"))
  expect_no_error(syn_dir_to_dflist("syn50996868", "fitbit"))
  expect_no_error(syn_dir_to_dflist("syn50996868", c("fitbit")))
  expect_no_error(syn_dir_to_dflist("syn50996868", c("fitbit", "healthkit")))
})
