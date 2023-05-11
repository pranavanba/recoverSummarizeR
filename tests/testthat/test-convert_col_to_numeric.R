test_that("conversion occurs correctly for default argument values", {
  my_dfs <- list(data.frame(value = c("1", "2", "3")),
                 data.frame(value = c("4", "5", "6")),
                 data.frame(value = c("7", "8", "9")))
  
  names(my_dfs) <- c("1", "2", "3")
  
  expect_type(my_dfs[[1]]$value, "character")
  expect_equal(class(my_dfs[[1]]$value), "character")
  
  my_dfs <- convert_col_to_numeric(my_dfs)
  
  expect_type(my_dfs[[1]]$value, "double")
  expect_equal(class(my_dfs[[1]]$value), "numeric")
})

test_that("conversion occurs correctly for custom argument values", {
  my_list <- list(df1 = data.frame(my_col = c("1", "2", "3")),
                  df2 = data.frame(my_col = c("4", "5", "6")),
                  exclude_df = data.frame(my_col = c("7", "8", "9")))
  
  expect_type(my_list[[1]]$my_col, "character")
  expect_type(my_list[[2]]$my_col, "character")
  expect_type(my_list[[3]]$my_col, "character")
  expect_equal(class(my_list[[1]]$my_col), "character")
  expect_equal(class(my_list[[2]]$my_col), "character")
  expect_equal(class(my_list[[3]]$my_col), "character")
  
  my_list <- convert_col_to_numeric(my_list, "exclude", "my_col")
  
  expect_type(my_list[[1]]$my_col, "double")
  expect_type(my_list[[2]]$my_col, "double")
  expect_type(my_list[[3]]$my_col, "character")
  expect_equal(class(my_list[[1]]$my_col), "numeric")
  expect_equal(class(my_list[[2]]$my_col), "numeric")
  expect_equal(class(my_list[[3]]$my_col), "character")
})

test_that("custom argument values are valid", {
  my_list <- list(df1 = data.frame(my_col = c("1", "2", "3")),
                  df2 = data.frame(my_col = c("4", "5", "6")),
                  exclude_df = data.frame(my_col = c("7", "8", "9")))
  
  expect_error(convert_col_to_numeric(my_list, "exclude", "col1"))
})

test_that("non-list input raises an error", {
  df1 <- data.frame(my_col = 1:3, b = letters[1:3],
                    value = 4:6, c = letters[4:6],
                    d = 7:9, z = letters[7:9])
  
  expect_error(convert_col_to_numeric(1))
  expect_error(convert_col_to_numeric("df1"))
  expect_error(convert_col_to_numeric(NA))
  expect_error(convert_col_to_numeric(df1))
  expect_error(convert_col_to_numeric(list(df1)))
})
