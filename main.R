# Import Libraries --------------------------------------------------------

# install.packages("install.load")
library(install.load)
install_load("dplyr","tidyr", "magrittr", "tibble", "devtools", "jsonlite", "stringr")
# install_github(
#   repo="https://github.com/generalui/synapser/tree/reticulate",
#   ref="reticulate")
library(synapser)


# Get data ----------------------------------------------------------------

synLogin()

dir.create('raw-data')
setwd('raw-data/')
system("synapse get -r syn50996868")
setwd('..')


# Parse file(s) to dataframe -------------------------------------------------
# File format depends on what is available in the data export from Care
# Evolution (e.g. it can be CSV and/or JSON)

# DailyData_json <- stream_in(con = file('raw-data/FitbitDailyData_20220111-20230103.json'))
DailyData_csv <- as_tibble(read.csv('raw-data/FitbitDailyData_20221101-20230103.csv'))


# Process i2b2 concept map ------------------------------------------------

concept_map <- as_tibble(read.csv('i2b2conceptmap.csv', skip = 1))


# Convert to i2b2 concept format -------------------------------------------------
## Specific example using one concept =====================================
# Can use any DailyData concept
path <- concept_map$concept_cd[4]

example_df <- DailyData_csv %>% 
  select(ParticipantIdentifier, Date, BodyWeight) %>% 
  mutate(unit = concept_map$UNITS_CD[which(concept_map$concept_cd==path)]) %>% 
  mutate(valtype = typeof(BodyWeight)) %>% 
  mutate(definition = concept_map$Definition[which(concept_map$concept_cd==path)])


## Function for all concepts ==============================================
# Can use for all DailyData concepts

# 1) Filter the 'map' data frame to include rows only where 'concept' ends with
# 'export:concept' 
# 2) Create the final data frame by binding the relevant
# columns from 'data' data frame with those from the filtered map

fx <- function(concept, export, path_to_map_csv, path_to_data_csv) {
  data <- as_tibble(read.csv(data_path))
  
  map <- as_tibble(read.csv(map_csv, skip = 1))
  
  map_filtered <- map %>% 
    filter(str_ends(concept_cd, paste0(export, ":", concept))) %>% 
    select(concept_cd, UNITS_CD, Definition)

  out <- data %>% 
    select(ParticipantIdentifier, Date, concept) %>% 
    mutate(value_type = typeof(.[[3]])) %>% 
    bind_cols(select(map_filtered, UNITS_CD, Definition))
  
  # return(out)
}


# Test the function -------------------------------------------------------

BodyBmi <- fx("BodyBmi", "DailyData", 'i2b2conceptmap.csv', 'raw-data/FitbitDailyData_20221101-20230103.csv')
Calories <- fx("Calories", "DailyData", 'i2b2conceptmap.csv', 'raw-data/FitbitDailyData_20221101-20230103.csv')
BodyWeight <- fx("BodyWeight", "DailyData", 'i2b2conceptmap.csv', 'raw-data/FitbitDailyData_20221101-20230103.csv')


# Mock example for single df output ---------------------------------------

mock <- bind_rows(BodyBmi, BodyWeight, Calories)
mock %<>% rename(Value = Variable)
mock %<>% mutate(Variable = "")
mock$Variable[which(grepl("Index", mock$Definition))] <- "BodyBmi"
mock$Variable[which(grepl("weight", mock$Definition))] <- "BodyWeight"
mock$Variable[which(grepl("exercise", mock$Definition))] <- "Calories"
mock %<>% select(ParticipantIdentifier, Date, Variable, Value, value_type, UNITS_CD, Definition)
bind_rows(mock[1:5,], mock[483:487,], mock[965:969,]) %>% View()

