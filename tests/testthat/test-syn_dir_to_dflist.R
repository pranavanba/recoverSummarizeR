test_that("function works as expected", {
  synapser::synLogin()
  
  expect_no_error(syn_dir_to_dflist("syn50996868", "./test-synget"))
  expect_no_error(syn_dir_to_dflist("syn50996868", "./test-synget", "fitbit"))
  expect_no_error(syn_dir_to_dflist("syn50996868", "./test-synget", c("fitbit")))
  expect_no_error(syn_dir_to_dflist("syn50996868", "./test-synget", c("fitbit", "healthkit")))
})
