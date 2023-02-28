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
  "readr",
  "reshape2"
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

## Specific example using one concept

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


# Test the function

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

# 1. Store vector of metadata cols (all other cols will be treated as signal/measurement vars)

all_cols <- lapply(df_list, names) %>%
  unlist() %>%
  enframe()

all_cols$name %<>% {
  gsub("\\s\\d+", "", .)
}

tmp <- all_cols$value[all_cols$value %>%
  duplicated() %>%
  which()] %>%
  append(all_cols$value[grepl("date|modified", (all_cols$value %>% tolower()))]) %>%
  # append(all_cols$value[grepl("date", (all_cols$value %>% tolower())) & !grepl("HKDate", all_cols$value)]) %>%
  unique()

metadata_cols <- tmp[-grep("heartrate|calories|steps", (tmp %>% tolower()))]

rm(tmp)

# 2. Melt data frames (know which cols are metadata/measurement data based on list of cols created in previous step) and update structure: measurement (variable) and value (variable value) cols

# Define a function to split up a data frame
split_df <- function(df) {
  # Get indices of signal measurement columns
  signal_cols <- setdiff(names(df), metadata_cols)
  
  # Melt the data frame to create a single "value" column
  df_melt <- melt(df, id.vars = intersect(metadata_cols, names(df)), measure.vars = signal_cols,
                  variable.name = "measurement", value.name = "value")
  
  return(df_melt)
}

# Apply the split_df function to the list of data frames
new_df_list <- lapply(df_list, split_df)

new_factors <- lapply(new_df_list, function(x) x[["measurement"]] %>% unique())

# 3. Add i2b2 cols (unit, type, definition); OR copy/link appropriate section of respective data frames into new column in concept map? E.g. for second idea: value of new col `data` for row with concept_cd==MHP:Fitbit:DailyData:BodyBmi would be the subset of data matching BodyBmi data from fitbitdailydata data frame

# 4. Summarize data on specific time scales (weekly, all-time) for specified statistics (5/95 percentiles, mean, median, variance, number of records)
# For summaries: create fx for each type of summary --> all time vs weekly, add case switching within both depending on 5/95 percentile, mean, med, var, count

summary <- function(timescale, type) {
  alltime <- function(type) {
    switch(type,
      pct5 = new_df_list[["fitbitactivitylogs "]] %>%
        select(measurement, value) %>%
        group_by(measurement) %>%
        summarise(quantile(as.numeric(value), 0.05, na.rm = T)),
      pct95 = new_df_list[["fitbitactivitylogs "]] %>%
        select(measurement, value) %>%
        group_by(measurement) %>%
        select(value) %>%
        summarise(quantile(as.numeric(value), 0.95, na.rm = T)),
      mean = new_df_list[["fitbitactivitylogs "]] %>%
        select(measurement, value) %>%
        group_by(measurement) %>%
        summarise(mean(as.numeric(value), na.rm = T)),
      median = new_df_list[["fitbitactivitylogs "]] %>%
        select(measurement, value) %>%
        group_by(measurement) %>%
        summarise(median(as.numeric(value), na.rm = T)),
      variance = new_df_list[["fitbitactivitylogs "]] %>%
        select(measurement, value) %>%
        group_by(measurement) %>%
        summarise(var(as.numeric(value), na.rm = T)),
      numRecords = new_df_list[["fitbitactivitylogs "]] %>%
        select(measurement, value) %>%
        group_by(measurement) %>%
        drop_na() %>%
        count()
    )
  }

  weekly <- function(type) {
    switch(type,
      pct5 = ,
      pct95 = ,
      mean = ,
      median = ,
      variance = ,
      numRecords = 
    )
  }

  switch(timescale,
    alltime = alltime(type),
    weekly = weekly(type)
  )
}

summary("alltime", "median")

