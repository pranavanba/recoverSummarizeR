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
  unzip(exdir = 'raw-data')


# Parse file(s) to dataframe -------------------------------------------------
# File format depends on what is available in the data export from Care Evolution (e.g. it can be CSV and/or JSON)
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
fx <- function(concept, export, map, data) {
  path <- map %>% 
    select(concept_cd) %>% 
    filter(grepl(paste0("Fitbit:",export,":",concept,"$"), concept_cd, ignore.case = T)) %>% 
    as.character()
  
  concept <- data %>% 
    select(ParticipantIdentifier, Date, concept) %>% 
    # mutate(unit = map$UNITS_CD[which(map$concept_cd==path)]) %>%
    mutate(valtype = typeof(.[[3]]))
    # mutate(definition = map$Definition[which(map$concept_cd==path)])
  
  # return(concept)
}


# Test the function -------------------------------------------------------


BodyBmi <- fx("BodyBmi", "DailyData", concept_map, DailyData_csv)
Calories <- fx("Calories", "DailyData", concept_map, DailyData_csv)
BodyWeight <- fx("BodyWeight", "DailyData", concept_map, DailyData_csv)

