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
  "reshape2",
  "lubridate"
)
# install_github(
#   repo="https://github.com/generalui/synapser/tree/reticulate",
#   ref="reticulate")
library(synapser)


# Get data ----------------------------------------------------------------

synLogin()

dir.create("raw-data")
setwd("raw-data/")
dir.create("parquet-datasets")
setwd("parquet-datasets/")
system("synapse get -r syn50996868")
setwd("..")
setwd("..")


# Get i2b2 concepts map ---------------------------------------------------

concept_map <-
  googlesheets4::read_sheet(
    "https://docs.google.com/spreadsheets/d/1XagFptBLxk5UW5CzZl-2gA8ncqwWk6XcGVFFSna2R_s/edit?usp=share_link"
  )
1

concept_map$concept_cd %<>% tolower()

break

# Read parquet files to df ------------------------------------------------

parent_directory <- "raw-data/parquet-datasets"

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

# Clean up names of parquet datasets
names(tmp) <-
  gsub("\\.(parquet|tsv|ndjson)$",
       "",
       paste(basename(dirname(file_paths)), "-", basename(file_paths)))
names(tmp) <- gsub("(dataset_|-.*\\.snappy| )", "", names(tmp))

# Include only fitbit datasets for now
tmp <- tmp[grepl("fitbit", tolower(names(tmp)))]
tmp <- tmp[!grepl("manifest", tolower(names(tmp)))]

# May need to update the following to accommodate multiple multi-part parquet
# files; currently works with only one mutli-part file: fitbitintradaycombined
# was the only multi-part parquet file found. Perhaps combine the multi-part
# files inside the respective raw-data nested folder before reading all into a
# df.
multi_part_dfs <- list(names(tmp)[duplicated(names(tmp))] %>% unique())
names(multi_part_dfs) <- names(tmp)[duplicated(names(tmp))] %>% unique()

tmp_fitbitintradaycombined <- bind_rows(tmp[which(names(tmp) == multi_part_dfs)])

tmp2 <- tmp[!grepl(multi_part_dfs, names(tmp))]

df_list <- c(tmp2, list(tmp_fitbitintradaycombined))

names(df_list)[(df_list %>% names() %>% {(nchar(.)==0)} %>% which())] <- 
  names(multi_part_dfs)

# names(df_list)[which(!(unique(names(df_list)) %in% unique(names(tmp))))] <-
#   unique(names(tmp))[which(!(unique(names(tmp)) %in% unique(names(df_list))))]

rm(parent_directory, file_paths, tmp, multi_part_dfs, tmp_fitbitintradaycombined,  tmp2)

# Data Summarization ----------------------------------------------------------------------------------------------

# 1. Store approved and excluded concepts columns

all_cols <- lapply(df_list, names) %>%
  unlist() %>%
  enframe()

all_cols$name %<>% {
  gsub("\\s\\d+|\\d", "", .)
}

approved_concepts <- 
  concept_map$concept_cd %>%
  grep("^(?!.*Trigger).+$", ., perl = T, value = T) %>% 
  str_extract("(?<=:)[^:]*$") %>% unique()

approved_concepts %<>% 
  {gsub("mins", "minutes", .)} %>% 
  {gsub("avghr", "averageheartrate", .)} %>% 
  {gsub("spo2(?!_)", "spo2_", ., perl = T)} %>% 
  {gsub("brth", "breath", .)} %>% 
  {gsub("hrvd", "hrv_d", .)} %>% 
  {gsub("restinghr$", "restingheartrate", ., perl = T)} %>% 
  {gsub("sleepbreath", "sleepsummarybreath", .)} %>% 
  unique()

excluded_concepts <- all_cols$value[!(tolower(all_cols$value) %in% tolower(approved_concepts))] %>% unique()

# excluded_concepts <- all_cols$value[all_cols$value %>% duplicated() %>% which()] %>%
#   append(all_cols$value[grepl("date|modified", (all_cols$value %>% tolower()))]) %>%
#   unique()

# excluded_concepts <- excluded_concepts[-grep("heartrate|calories|steps|duration|dateofbirth", (excluded_concepts %>% tolower()))]

excluded_concepts <- excluded_concepts[!(tolower(excluded_concepts) %in% tolower(approved_concepts))]

# rm(excluded_concepts)

# 2. Melt data frames from wide to long with new concept (variable) and value (variable value) cols (we know which cols are approved or excluded based on lists of concepts created in previous step)

# Define a function to melt a data frame
melt_df <- function(df) {
  # Get indices of approved concept columns
  approved_cols <- setdiff(names(df), excluded_concepts)
  
  # Melt the data frame to create a single "value" column
  df_melt <- melt(df, id.vars = intersect(excluded_concepts, names(df)), measure.vars = approved_cols,
                  variable.name = "concept", value.name = "value")
  
  return(df_melt)
}

# Apply the melt_df function to the list of data frames
new_df_list <- lapply(df_list, melt_df)

new_factors <- lapply(new_df_list, function(x) x[["concept"]] %>% unique())

filtered_df_list <- 
  lapply(new_df_list, function(x) {
    x %>% 
      select(if ("ParticipantIdentifier" %in% colnames(x)) "ParticipantIdentifier",
             matches("(?<!_)date(?!_)", perl = T),
             if ("concept" %in% colnames(x)) "concept",
             if ("value" %in% colnames(x)) "value")
    })

# # Convert "value" column to numeric
# convert_column_to_numeric <- function(x) {
#   if (grepl("devices", deparse(substitute(x)))) {
#     return(x)
#     } else {
#       if ("value" %in% names(x)) {
#         x$value <- as.numeric(x$value)
#         }
#       return(x)
#     }
#   }
# 
# filtered_df_list <- lapply(filtered_df_list, convert_column_to_numeric)

filtered_df_list$fitbitactivitylogs$value %<>% as.numeric()
filtered_df_list$fitbitdailydata$value %<>% as.numeric()
filtered_df_list$fitbitrestingheartrates$value %<>% as.numeric()
filtered_df_list$fitbitsleeplogs$value %<>% as.numeric()
filtered_df_list$fitbitintradaycombined$value %<>% as.numeric()

# 3. Format non-summarized data as per i2b2 specs

# new_df_list$enrolledparticipants %<>% 
#   mutate(concept = paste0("mhp:survey:enrolledparticipants:", concept))

# add_i2b2_prefix <- function(dataset) {
#   dataset_name <- deparse(substitute(dataset))
#   
#   if (grepl("\\$fitbit", dataset_name)) {
#     dataset_name <- sub("^.*\\$fitbit", "fitbit:", dataset_name)
#   } else if (grepl("\\$healthkitv2", dataset_name)) {
#     dataset_name <- sub("^.*\\$healthkitv2", "healthkit:", dataset_name)
#   } else if (grepl("symptomlog$", dataset_name)) {
#     dataset_name <- sub("^.*\\$symptomlog", "symptomlog", dataset_name)
#   } else if (grepl("symptomlog_value_s", dataset_name)) {
#     dataset_name <- sub("^.*\\$symptomlog_value_symptoms", "symptomlog:symptoms", dataset_name)
#   } else if (grepl("symptomlog_value_t", dataset_name)) {
#     dataset_name <- sub("^.*\\$symptomlog_value_treatments", "symptomlog:treatments", dataset_name)
#   } else {
#     dataset_name <- gsub("^.*\\$", "survey:", dataset_name)
#   }
# 
#   if ("concept" %in% colnames(dataset)) {
#     result <- 
#       dataset %>% 
#       mutate(concept = paste0("mhp:", dataset_name, ":", concept))
#     } else {
#       result <- dataset
#     }
#   
#   return(result)
# }

# tmpout_non_summarized <- 
#   add_i2b2_prefix(new_df_list$fitbitactivitylogs) %>% 
#   select(ParticipantIdentifier, matches("(?<!_)date(?!_)", perl = T), concept, value)

# tmpout_non_summarized <- 
#   lapply(new_df_list, add_i2b2_prefix) %>% 
#   select(if ("ParticipantIdentifier" %in% colnames(x)) "ParticipantIdentifier", 
#          matches("(?<!_)date(?!_)", perl = T), 
#          if ("concept" %in% colnames(x)) "concept", 
#          if ("value" %in% colnames(x)) "value")


# 4. Summarize data on specific time scales (weekly, all-time) for specified statistics (5/95 percentiles, mean, median, variance, number of records)

summary <- function(dataset) {
  
  all_pct5 <- 
    dataset %>%
    select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
    group_by(ParticipantIdentifier, concept) %>%
    mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
    select(-c(value)) %>%
    rename(value = quantile) %>%
    mutate(
      StartDate = as_datetime(min(StartDate)),
      EndDate = as_datetime(max(EndDate)),
      timescale = "alltime",
      stat = "5pct",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    distinct() %>%
    ungroup() %>%
    select(-c(timescale, stat))
  
  all_pct95 <- 
    dataset %>%
    select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
    group_by(ParticipantIdentifier, concept) %>%
    mutate("quantile" = quantile(as.numeric(value), 0.95, na.rm = T)) %>%
    select(-c(value)) %>%
    rename(value = quantile) %>%
    mutate(
      StartDate = as_datetime(min(StartDate)),
      EndDate = as_datetime(max(EndDate)),
      timescale = "alltime",
      stat = "95pct",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    distinct() %>%
    ungroup() %>%
    select(-c(timescale, stat))
  
  all_mean <- 
    dataset %>%
    select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
    group_by(ParticipantIdentifier, concept) %>%
    mutate("mean" = mean(as.numeric(value), na.rm = T)) %>%
    select(-c(value)) %>%
    rename(value = mean) %>%
    mutate(
      StartDate = as_datetime(min(StartDate)),
      EndDate = as_datetime(max(EndDate)),
      timescale = "alltime",
      stat = "mean",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    distinct() %>%
    ungroup() %>%
    select(-c(timescale, stat))
  
  all_median <- 
    dataset %>%
    select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
    group_by(ParticipantIdentifier, concept) %>%
    mutate("median" = median(as.numeric(value), na.rm = T)) %>%
    select(-c(value)) %>%
    rename(value = median) %>%
    mutate(
      StartDate = as_datetime(min(StartDate)),
      EndDate = as_datetime(max(EndDate)),
      timescale = "alltime",
      stat = "median",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    distinct() %>%
    ungroup() %>%
    select(-c(timescale, stat))
  
  all_variance <- 
    dataset %>%
    select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
    group_by(ParticipantIdentifier, concept) %>%
    mutate("variance" = var(as.numeric(value), na.rm = T)) %>%
    select(-c(value)) %>%
    rename(value = variance) %>%
    mutate(
      StartDate = as_datetime(min(StartDate)),
      EndDate = as_datetime(max(EndDate)),
      timescale = "alltime",
      stat = "variance",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    distinct() %>%
    ungroup() %>%
    select(-c(timescale, stat))
  
  all_numrecords <- 
    dataset %>%
    select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
    group_by(ParticipantIdentifier, concept) %>%
    drop_na() %>%
    add_count() %>%
    select(-c(value)) %>%
    rename(value = n) %>%
    mutate(
      StartDate = as_datetime(min(StartDate)),
      EndDate = as_datetime(max(EndDate)),
      timescale = "alltime",
      stat = "numrecords",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    distinct() %>%
    ungroup() %>%
    select(-c(timescale, stat))
  
  weekly_pct5 <- 
    dataset %>%
    select(ParticipantIdentifier, concept, value, StartDate) %>%
    mutate(
      StartDate = ymd_hms(StartDate),
      year = year(StartDate),
      week = epiweek(StartDate)
    ) %>%
    filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
    group_by(ParticipantIdentifier, concept, year, week) %>%
    summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T),
              .groups = "keep") %>%
    ungroup() %>%
    mutate(
      week_summary_start_date =
        make_date(year, 1, 1) +
        weeks(week - 1) +
        days(7 - wday(make_date(year, 1, 1)) + 1),
      week_summary_end_date =
        week_summary_start_date +
        weeks(1) -
        days(1),
      timescale = "weekly",
      stat = "5pct",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    select(-c(year, week)) %>%
    select(
      ParticipantIdentifier,
      week_summary_start_date,
      week_summary_end_date,
      concept,
      value
    ) %>%
    rename(StartDate = week_summary_start_date) %>%
    rename(EndDate = week_summary_end_date)
  
  weekly_pct95 <- 
    dataset %>%
    select(ParticipantIdentifier, concept, value, StartDate) %>%
    mutate(
      StartDate = ymd_hms(StartDate),
      year = year(StartDate),
      week = epiweek(StartDate)
    ) %>%
    filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
    group_by(ParticipantIdentifier, concept, year, week) %>%
    summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T),
              .groups = "keep") %>%
    ungroup() %>%
    mutate(
      week_summary_start_date =
        make_date(year, 1, 1) +
        weeks(week - 1) +
        days(7 - wday(make_date(year, 1, 1)) + 1),
      week_summary_end_date =
        week_summary_start_date +
        weeks(1) -
        days(1),
      timescale = "weekly",
      stat = "95pct",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    select(-c(year, week)) %>%
    select(
      ParticipantIdentifier,
      week_summary_start_date,
      week_summary_end_date,
      concept,
      value
    ) %>%
    rename(StartDate = week_summary_start_date) %>%
    rename(EndDate = week_summary_end_date)
  
  weekly_mean <- 
    dataset %>%
    select(ParticipantIdentifier, concept, value, StartDate) %>%
    mutate(
      StartDate = ymd_hms(StartDate),
      year = year(StartDate),
      week = epiweek(StartDate)
    ) %>%
    filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
    group_by(ParticipantIdentifier, concept, year, week) %>%
    summarise("value" = mean(as.numeric(value), na.rm = T), .groups = "keep") %>%
    ungroup() %>%
    mutate(
      week_summary_start_date =
        make_date(year, 1, 1) +
        weeks(week - 1) +
        days(7 - wday(make_date(year, 1, 1)) + 1),
      week_summary_end_date =
        week_summary_start_date +
        weeks(1) -
        days(1),
      timescale = "weekly",
      stat = "mean",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    select(-c(year, week)) %>%
    select(
      ParticipantIdentifier,
      week_summary_start_date,
      week_summary_end_date,
      concept,
      value
    ) %>%
    rename(StartDate = week_summary_start_date) %>%
    rename(EndDate = week_summary_end_date)
  
  weekly_median <- 
    dataset %>%
    select(ParticipantIdentifier, concept, value, StartDate) %>%
    mutate(
      StartDate = ymd_hms(StartDate),
      year = year(StartDate),
      week = epiweek(StartDate)
    ) %>%
    filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
    group_by(ParticipantIdentifier, concept, year, week) %>%
    summarise("value" = median(as.numeric(value), na.rm = T), .groups = "keep") %>%
    ungroup() %>%
    mutate(
      week_summary_start_date =
        make_date(year, 1, 1) +
        weeks(week - 1) +
        days(7 - wday(make_date(year, 1, 1)) + 1),
      week_summary_end_date =
        week_summary_start_date +
        weeks(1) -
        days(1),
      timescale = "weekly",
      stat = "median",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    select(-c(year, week)) %>%
    select(
      ParticipantIdentifier,
      week_summary_start_date,
      week_summary_end_date,
      concept,
      value
    ) %>%
    rename(StartDate = week_summary_start_date) %>%
    rename(EndDate = week_summary_end_date)
  
  weekly_variance <- 
    dataset %>%
    select(ParticipantIdentifier, concept, value, StartDate) %>%
    mutate(
      StartDate = ymd_hms(StartDate),
      year = year(StartDate),
      week = epiweek(StartDate)
    ) %>%
    filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
    group_by(ParticipantIdentifier, concept, year, week) %>%
    summarise("value" = var(as.numeric(value), na.rm = T), .groups = "keep") %>%
    ungroup() %>%
    mutate(
      week_summary_start_date =
        make_date(year, 1, 1) +
        weeks(week - 1) +
        days(7 - wday(make_date(year, 1, 1)) + 1),
      week_summary_end_date =
        week_summary_start_date +
        weeks(1) -
        days(1),
      timescale = "weekly",
      stat = "variance",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    select(-c(year, week)) %>%
    select(
      ParticipantIdentifier,
      week_summary_start_date,
      week_summary_end_date,
      concept,
      value
    ) %>%
    rename(StartDate = week_summary_start_date) %>%
    rename(EndDate = week_summary_end_date)
  
  weekly_numrecords <-
    dataset %>%
    select(ParticipantIdentifier, concept, value, StartDate) %>%
    mutate(
      StartDate = ymd_hms(StartDate),
      year = year(StartDate),
      week = epiweek(StartDate)
    ) %>%
    filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
    group_by(ParticipantIdentifier, concept, year, week) %>%
    drop_na() %>%
    count() %>%
    rename(value = n) %>%
    ungroup() %>%
    mutate(
      week_summary_start_date =
        make_date(year, 1, 1) +
        weeks(week - 1) +
        days(7 - wday(make_date(year, 1, 1)) + 1),
      week_summary_end_date =
        week_summary_start_date +
        weeks(1) -
        days(1),
      timescale = "weekly",
      stat = "numrecords",
      concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
    ) %>%
    select(-c(year, week)) %>%
    select(
      ParticipantIdentifier,
      week_summary_start_date,
      week_summary_end_date,
      concept,
      value
    ) %>%
    rename(StartDate = week_summary_start_date) %>%
    rename(EndDate = week_summary_end_date)
  
  result <-
    bind_rows(
      all_pct5,
      all_pct95,
      all_mean,
      all_median,
      all_variance,
      all_numrecords,
      weekly_pct5,
      weekly_pct95,
      weekly_mean,
      weekly_median,
      weekly_variance,
      weekly_numrecords
    )
  
  return(result)
}

tmpout_summarized <- summary(new_df_list$fitbitactivitylogs)

# 6. Update output to match concept map format

tmpout_summarized$concept %<>% 
  tolower() %>% 
  {gsub("sleepsummarybreath", "sleepbreath", .)} %>% 
  {gsub("restingheartrate", "restinghr", ., perl = T)} %>% 
  {gsub("hrv_d", "hrvd", .)} %>% 
  {gsub("breath", "brth", .)} %>% 
  {gsub("spo2_", "spo2", ., perl = T)} %>% 
  {gsub("averageheartrate", "avghr", .)} %>% 
  {gsub("minutes", "mins", .)}

tmpout_summarized %<>% 
  mutate(StartDate = as_date(StartDate),
         EndDate = as_date(EndDate)) %>% 
  mutate(valtype_cd = class(value)) %>% 
  mutate(nval_num = as.numeric(case_when(valtype_cd == "numeric" ~ value)),
         tval_char = as.character(case_when(valtype_cd == "character" ~ value))) %>% 
  select(-value)

tmpout_summarized %<>% 
  left_join(select(concept_map, concept_cd, UNITS_CD), 
            by = c("concept" = "concept_cd")) # Add units_cd column from concept map matching rows by concept strings

colnames(tmpout_summarized) <- tolower(colnames(tmpout_summarized))

# Write out output ------------------------------------------------------------------------------------------------

# 1. Output data frames as CSVs to 'deliverables' folder

out_dir <- "deliverables"
dir.create(out_dir)

write.csv(tmpout_summarized, file = 'deliverables/summarized_concepts.csv', row.names = F)
write.csv(tmpout_non_summarized, file = 'deliverables/non_summarized_concepts.csv', row.names = F)
write.csv(concept_map, file = 'deliverables/concepts_map.csv', row.names = F)

# 2. Store data frames as tables in Synapse
tmp <- synBuildTable("summarized_concepts.csv", "syn43435581", tmpout_summarized)
tmp <- synStore(tmp)
rm(tmp)

tmp <- synBuildTable("concepts_map.csv", "syn43435581", concepts_map)
tmp <- synStore(tmp)
rm(tmp)



