# Import Libraries --------------------------------------------------------

# install.packages("install.load")
library(install.load)
install_load("dplyr","tidyr", "magrittr", "tibble", "devtools")
# install_github(
#   repo="https://github.com/generalui/synapser/tree/reticulate",
#   ref="reticulate")
library(synapser)


# Read data ---------------------------------------------------------------

synLogin()

file.id <- "syn50894469"

file.id %>% 
  synGet() %>% 
  {.$path} %>% 
  unzip(exdir = 'data')
