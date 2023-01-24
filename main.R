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

file.id <- "syn50894469"

file.id %>% 
  synGet() %>% 
  {.$path} %>% 
  unzip(exdir = 'raw-data')


# Parse file(s) to dataframe -------------------------------------------------

DailyData_json <- stream_in(con = file('raw-data/FitbitDailyData_20220111-20230103.json'))
# DailyData_csv <- as_tibble(read.csv('raw-data/FitbitDailyData_20221101-20230103.csv'))


# Process i2b2 concept map ------------------------------------------------

concept_map <- as_tibble(read.csv('i2b2conceptmap.csv', skip = 1))


# Convert to i2b2 concept -------------------------------------------------

# Specific example using any DailyData concept
dd_path <- concept_map$concept_cd[4]

fitbit_dd_example <- DailyData_csv %>% 
  select(ParticipantIdentifier, Date, BodyWeight) %>% 
  # mutate(BodyBmi = as.numeric(BodyFat)) %>% 
  mutate(unit = concept_map$UNITS_CD[which(concept_map$concept_cd==dd_path)]) %>% 
  mutate(valtype = typeof(BodyWeight)) %>% 
  mutate(definition = concept_map$Definition[which(concept_map$concept_cd==dd_path)])

# Function to use for all DailyData concepts
fx <- function(concept, map, data) {
  fact_path <- map %>% 
    select(concept_cd) %>% 
    filter(grepl(paste0("Fitbit:DailyData:",concept,"$"), concept_cd, ignore.case = T))
  
  concept <- data %>% 
    select(ParticipantIdentifier, Date, concept) %>% 
    # mutate(unit = map$UNITS_CD[which(map$concept_cd==fact_path)]) %>%
    mutate(valtype = typeof(.[[3]]))
    # mutate(definition = map$Definition[which(map$concept_cd==fact_path)])
  
  # return(concept)
}

BodyBmi <- fx("BodyBmi", concept_map, DailyData_csv)
Calories <- fx("Calories", concept_map, DailyData_csv)
BodyWeight <- fx("BodyWeight", concept_map, DailyData_csv)

