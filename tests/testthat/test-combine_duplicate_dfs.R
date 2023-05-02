test_that("combining data frames works", {
  df1 <- data.frame(x = 1:3, y = letters[1:3])
  df2 <- data.frame(x = 4:6, y = letters[4:6])
  df3 <- data.frame(x = 7:9, y = letters[7:9])
  df4 <- data.frame(x = 10:12, y = letters[10:12])
  df_list <- list(df1, df2, df1, df3, df4, df2)
  names(df_list) <- c("df1", "df2", "df1", "df3", "df4", "df2")
  combined_df_list <- combine_duplicate_dfs(df_list)
  
  expect_equal(combined_df_list$df1, df1 %>% dplyr::bind_rows(df1))
  expect_equal(combined_df_list$df2, df2 %>% dplyr::bind_rows(df2))
  expect_equal(combined_df_list$df3, df3)
  expect_equal(combined_df_list$df4, df4)
  expect_length(combined_df_list, 4)
})

test_that("length of output is correct", {
  df1 <- data.frame(x = 1:3, y = letters[1:3])
  df2 <- data.frame(x = 4:6, y = letters[4:6])
  df3 <- data.frame(x = 7:9, y = letters[7:9])
  df4 <- data.frame(x = 10:12, y = letters[10:12])
  df_list <- list(df1, df2, df1, df3, df4, df2)
  names(df_list) <- c("df1", "df2", "df1", "df3", "df4", "df2")
  combined_df_list <- combine_duplicate_dfs(df_list)
  
  n <- length(names(df_list))
  d <- sum(names(df_list) == names(df_list)[duplicated(names(df_list))])
  u <- sum(duplicated(names(df_list)))
  
  expect_length(combined_df_list, n-d+u)
})

test_that("non-list input raises an error", {
  df1 <- data.frame(a = 1:3, b = letters[1:3],
                    a = 4:6, c = letters[4:6],
                    d = 7:9, b = letters[7:9])
  names(df1) <- c("a", "b", "a", "c", "d", "b")
  
  second <- data.frame(a = 1:3, b = letters[1:3],
                    a = 4:6, c = letters[4:6],
                    d = 7:9, b = letters[7:9])  
  
  expect_error(combine_duplicate_dfs(df1))
  expect_error(combine_duplicate_dfs(list(second)))
  expect_error(combine_duplicate_dfs("df1"))
  expect_error(combine_duplicate_dfs(1))
  expect_error(combine_duplicate_dfs(NA))
})
