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

DailyData_json <- stream_in(con = file('data/FitbitDailyData_20220111-20230103.json'))
DailyData_csv <- as_tibble(read.csv('data/FitbitDailyData_20221101-20230103.csv'))


# Process i2b2 concept map ------------------------------------------------

concept_map <- as_tibble(read.csv('i2b2conceptmap.csv', skip = 1))


# Convert to i2b2 concept -------------------------------------------------
# Specific examples with a few concepts

# Body BMI
bodybmi_path <- concept_map$concept_cd[1]

fitbit_bodybmi <- DailyData_csv %>% 
  select(ParticipantIdentifier, Date, BodyBmi) %>% 
  mutate(BodyBmi = as.numeric(BodyBmi)) %>% 
  mutate(unit = concept_map$UNITS_CD[which(concept_map$concept_cd==bodybmi_path)]) %>% 
  mutate(valtype = typeof(BodyBmi)) %>% 
  mutate(definition = concept_map$Definition[which(concept_map$concept_cd==bodybmi_path)])


