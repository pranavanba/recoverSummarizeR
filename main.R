# Import Libraries --------------------------------------------------------

# install.packages("install.load")
library(install.load)
install_load(
  "dplyr",
  "tidyr",
  "magrittr",
  "tibble",
  "devtools",
  "jsonlite",
  "stringr",
  "arrow",
  "googlesheets4",
  "readr"
)
# install_github(
#   repo="https://github.com/generalui/synapser/tree/reticulate",
#   ref="reticulate")
library(synapser)


# Get data ----------------------------------------------------------------

synLogin()

dir.create("raw-data")
setwd("raw-data/")
dir.create("parquet-pilot-datasets")
setwd("parquet-pilot-datasets/")
system("synapse get -r syn50996868")
setwd("..")
setwd("..")


# Get i2b2 concepts map ---------------------------------------------------

concept_map <-
  googlesheets4::read_sheet(
    "https://docs.google.com/spreadsheets/d/1XagFptBLxk5UW5CzZl-2gA8ncqwWk6XcGVFFSna2R_s/edit?usp=share_link"
  )


# Read parquet files to df ------------------------------------------------

parent_directory <- "raw-data/parquet-pilot-datasets"

file_paths <-
  list.files(path = parent_directory,
             recursive = TRUE,
             full.names = TRUE)

tmp <- lapply(file_paths, function(file_path) {
  if (grepl(".parquet$", file_path)) {
    read_parquet(file_path)
  } else if (grepl(".tsv$", file_path)) {
    read_tsv(file_path, show_col_types = F)
  } else if (grepl(".ndjson$", file_path)) {
    ndjson::stream_in(file_path, cls = "tbl")
  }
})

names(tmp) <-
  gsub("\\.(parquet|tsv|ndjson)$",
       "",
       paste(basename(dirname(file_paths)), "-", basename(file_paths)))

# Exclude manifest and HK ecg parent files for now
tmp <- tmp[!grepl("MANIFEST", names(tmp))]
tmp <-
  tmp[!grepl("healthkitv2electrocardiogram - HealthKitV2Electrocardiogram",
             names(tmp))]
names(tmp) <- sub("-.*\\.snappy", "", names(tmp))
names(tmp) <- sub("dataset_", "", names(tmp))

# May need to update the following to accommodate multiple multi-part parquet
# files; currently works with only one mutli-part file: fitbitintradaycombined
# was the only multi-part parquet file found. Perhaps combine the multi-part
# files inside the respective raw-data nested folder before reading all into a
# df.
multi_part_dfs <-
  list(names(tmp)[duplicated(names(tmp))] %>% unique())
tmp_df_list <- bind_rows(tmp[which(names(tmp) == multi_part_dfs)])
tmp2 <- tmp[!grepl(multi_part_dfs, names(tmp))]
df_list <- c(tmp2, list(tmp_df_list))
names(df_list)[which(!(unique(names(df_list)) %in% unique(names(tmp))))] <-
  unique(names(tmp))[which(!(unique(names(tmp)) %in% unique(names(df_list))))]
rm(tmp, tmp2, tmp_df_list, multi_part_dfs)


# Convert to i2b2 concept format -------------------------------------------------

## Specific example using one concept =====================================

# Can use any DailyData concept
path <- concept_map$concept_cd[4]

example_df <- DailyData_csv %>%
  select(ParticipantIdentifier, Date, BodyWeight) %>%
  mutate(unit = concept_map$UNITS_CD[which(concept_map$concept_cd == path)]) %>%
  mutate(valtype = typeof(BodyWeight)) %>%
  mutate(definition = concept_map$Definition[which(concept_map$concept_cd ==
                                                     path)])


## Function for all concepts ==============================================

# Can use for all DailyData concepts

# 1) Filter the 'map' data frame to include rows only where 'concept' ends with
# 'export:concept'
# 2) Create the final data frame by binding the relevant
# columns from 'data' data frame with those from the filtered map

fx <- function(concept,
               export,
               path_to_map_csv,
               path_to_data_csv) {
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

BodyBmi <-
  fx(
    "BodyBmi",
    "DailyData",
    "i2b2conceptmap.csv",
    "raw-data/FitbitDailyData_20221101-20230103.csv"
  )
Calories <-
  fx(
    "Calories",
    "DailyData",
    "i2b2conceptmap.csv",
    "raw-data/FitbitDailyData_20221101-20230103.csv"
  )
BodyWeight <-
  fx(
    "BodyWeight",
    "DailyData",
    "i2b2conceptmap.csv",
    "raw-data/FitbitDailyData_20221101-20230103.csv"
  )


# Mock example for single df output ---------------------------------------

mock <- bind_rows(BodyBmi, BodyWeight, Calories)
mock %<>% rename(Value = Variable)
mock %<>% mutate(Variable = "")
mock$Variable[which(grepl("Index", mock$Definition))] <- "BodyBmi"
mock$Variable[which(grepl("weight", mock$Definition))] <-
  "BodyWeight"
mock$Variable[which(grepl("exercise", mock$Definition))] <-
  "Calories"
mock %<>% select(ParticipantIdentifier,
                 Date,
                 Variable,
                 Value,
                 value_type,
                 UNITS_CD,
                 Definition)
bind_rows(mock[1:5,], mock[483:487,], mock[965:969,]) %>% View()


# Find common columns to merge on ---------------------------------------------------------------------------------

# Common cols between fitbit sleep log files
identical((df_list$`fitbitsleeplogs ` %>% count(LogId) %>% .[1] %>% distinct()),
          (df_list$`fitbitsleeplogs_sleeplogdetails ` %>% count(LogId) %>% .[1] %>% distinct())
)


# Data Summarization ----------------------------------------------------------------------------------------------

# 1. Extract collected and calculated vars from concept map and store someplace; OR
# 1. Store array of constant cols (all other cols will be treated as vars)

all_cols <-
  lapply(df_list, names) %>% 
  unlist() %>% 
  enframe()

all_cols$name %<>% {gsub("\\s\\d+", "", .)}
tmp <- all_cols$value[all_cols$value %>% duplicated() %>% which()] %>% unique()
const_cols <- tmp[-grep("heartrate|calories|steps", (tmp %>% tolower()))]

# 2. Separate dfs with vars into individual dfs (know which cols are constant and which are vars based on list of vars extracted in previous step)
# 3. Change structure: variable and value cols
# 4. Add i2b2 cols (unit, type, definition)
# 5. Row bind vars' dfs
