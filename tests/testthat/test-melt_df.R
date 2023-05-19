test_that("function works correctly", {
  melted_df <- melt_df(iris, c("Species"))
  
  expect_equal(typeof(iris), typeof(melted_df))
  expect_no_error(melt_df(mtcars))
  expect_error(melt_df())
})

test_that("incorrect input types raise an error", {
  expect_error(melt_df(list("a", "b", "c")))
  expect_error(melt_df(mtcars, 1))
})
