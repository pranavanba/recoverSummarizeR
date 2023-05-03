test_that("function works correctly", {
  df <- data.frame(participantidentifier = c(rep("A", 6), rep("B", 6)),
                   date = c("2022-01-01", "2022-01-05", "2022-01-10", "2022-02-01",
                            "2022-02-05", "2022-02-10", "2022-01-01", "2022-01-06",
                            "2022-01-11", "2022-02-01", "2022-02-06", "2022-02-11"),
                   concept = c("weight", "weight", "weight", "weight", "weight", "weight",
                               "height", "height", "height", "height", "height", "height"),
                   value = c(60, 62, 64, 65, 66, 68, 160, 162, 164, 165, 166, 168))
  
  df2 <- data.frame(participantidentifier = c(rep("A", 6), rep("B", 6)),
                   date = c("2022-01-01", "2022-01-05", "2022-01-10", "2022-02-01",
                            "2022-02-05", "2022-02-10", "2022-01-01", "2022-01-06",
                            "2022-01-11", "2022-02-01", "2022-02-06", "2022-02-11"),
                   concepts = c("weight", "weight", "weight", "weight", "weight", "weight",
                               "height", "height", "height", "height", "height", "height"),
                   value = c(60, 62, 64, 65, 66, 68, 160, 162, 164, 165, 166, 168))
  
  expect_no_error(stat_summarize(df))
  expect_error(stat_summarize(df2))
})

test_that("incorrect input type raises an error", {
  expect_error(stat_summarize("df"))
  expect_error(stat_summarize(list("a", "b")))
})
