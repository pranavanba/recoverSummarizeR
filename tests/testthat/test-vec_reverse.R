test_that("function works", {
  fruit_colors <- c(apple = "red", banana = "yellow", cherry = "red", kiwi = "brown", lemon = "yellow")
  expect_no_error(reverse_str_pairs(fruit_colors))
})

test_that("incorrect input types raise an error", {
  fruit_colors <- data.frame(fruit = c("apple", "banana", "cherry", "kiwi", "lemon"),
                             color = c("red", "yellow", "red", "brown", "yellow"))
  expect_error(vec_reverse(fruit_colors))
  expect_error(vec_reverse(1, 2))
  expect_error(vec_reverse(1))
})
