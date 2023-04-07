# Import Libraries --------------------------------------------------------

# install.packages("install.load")
library(install.load)
install_load(
  "magrittr",
  "googlesheets4"
)
library(synapser)


# Get data ----------------------------------------------------------------

synLogin()

if (dir.exists("raw-data")) {
  setwd("raw-data/")
  if (dir.exists("parquet-datasets")) {
    setwd("parquet-datasets/")
    system("synapse get -r syn50996868")
    setwd("..")
    setwd("..")
  } else {
    dir.create("parquet-datasets")
    setwd("parquet-datasets/")
    system("synapse get -r syn50996868")
    setwd("..")
    setwd("..")
  }
} else {
  dir.create("raw-data")
  setwd("raw-data/")
  dir.create("parquet-datasets")
  setwd("parquet-datasets/")
  system("synapse get -r syn50996868")
  setwd("..")
  setwd("..")
}

# Get i2b2 concepts map ---------------------------------------------------

get_concepts <- function(url, token_refresh) {
  x <- googlesheets4::read_sheet(url)
  token_refresh
  x$concept_cd %<>% tolower()
  return(x)
}

concepts_url <- "https://docs.google.com/spreadsheets/d/1XagFptBLxk5UW5CzZl-2gA8ncqwWk6XcGVFFSna2R_s/edit?usp=share_link"
concept_map <- get_concepts(concepts_url, token_refresh = 1)
rm(concepts_url)

