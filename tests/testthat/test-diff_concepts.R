test_that("function works correctly", {
  first1 <- data.frame(col_a = 1:5, col_b = letters[1:5], col_c = TRUE)
  second2 <- data.frame(col_b = letters[6:10], col_d = 6:10, col_e = FALSE)
  df_list <- list(first1, second2)
  names(df_list) <- c("first1", "second2")
  concept_map <- data.frame(concept = c("summary:a", "summary:b", "trigger:c"))
  concept_replacements <- c("a" = "col_a", "b" = "col_b")
  out <- diff_concepts(df_list = df_list,
                concept_replacements = concept_replacements,
                concept_map = concept_map,
                concept_filter_col = "concept")
  
  expect_setequal(out, c("col_c", "col_d", "col_e"))
  
})

test_that("incorrect input types raise an error", {
  df1 <- data.frame(my_col = 1:3, b = letters[1:3],
                    value = 4:6, c = letters[4:6],
                    d = 7:9, z = letters[7:9])
  
  expect_error(diff_concepts(df_list = 1,
                             concept_replacements = 1,
                             concept_map = 1,
                             concept_filter_col = 1))
  expect_error(diff_concepts(df_list = "df1", 
                             concept_replacements = "df1", 
                             concept_map = "df1", 
                             concept_filter_col = "df1"))
  expect_error(diff_concepts(df_list = NA, 
                             concept_replacements = NA, 
                             concept_map = NA, 
                             concept_filter_col = NA))
  expect_error(diff_concepts(df_list = df1, 
                             concept_replacements = df1, 
                             concept_map = df1, 
                             concept_filter_col = df1))
  expect_error(diff_concepts(df_list = list(df1), 
                             concept_replacements = list(df1), 
                             concept_map = list(df1), 
                             concept_filter_col = list(df1)))
})
