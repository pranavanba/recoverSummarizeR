# Import Libraries --------------------------------------------------------

# install.packages("install.load")
library(install.load)
install_load("dplyr","tidyr", "magrittr", "tibble", "devtools", "jsonlite")
# install_github(
#   repo="https://github.com/generalui/synapser/tree/reticulate",
#   ref="reticulate")
library(synapser)


# Read data ---------------------------------------------------------------

synLogin()

file.id <- "syn50894470"

file.id %>% 
  synGet() %>% 
  {.$path} %>% 
  unzip(exdir = 'data')


# Parse file(s) to dataframe -------------------------------------------------

tmpjson <- stream_in(con = file('data/FitbitActivityLogs_20220111-20230103.json'))
tmpcsv <- as_tibble(read.csv('data/FitbitActivityLogs_20221101-20230103.csv'))


# Process i2b2 concept map ------------------------------------------------

concept_map <- as_tibble(read.csv('i2b2conceptmap.csv', skip = 1))
