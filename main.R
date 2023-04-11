# Import Libraries --------------------------------------------------------

library(install.load)
install_load(
  "magrittr",
  "googlesheets4",
  "dplyr",
  "tidyr",
  "magrittr",
  "tibble",
  "devtools",
  "jsonlite",
  "stringr",
  "arrow",
  "readr",
  "reshape2",
  "lubridate",
  "purrr"
)
library(synapser)

# Get data ----------------------------------------------------------------

synLogin()

if(!dir.exists("raw-data")) {
  dir.create("raw-data")
}

setwd("raw-data/")

if(!dir.exists("parquet-datasets")) {
  dir.create("parquet-datasets")
}

setwd("parquet-datasets/")
system("synapse get -r syn50996868")
setwd("../..")

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

# Read parquet files to df ------------------------------------------------

parent_directory <- "raw-data/parquet-datasets"

file_paths <-
  list.files(path = parent_directory,
             recursive = T,
             full.names = T)

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
  file_paths %>% 
  {paste(basename(dirname(.)), "-", basename(.))} %>% 
  {gsub("\\.(parquet|tsv|ndjson)$|(dataset_|-.*\\.snappy| )", "", .)}

# Include only fitbit datasets for now
tmp <- tmp[grepl("fitbit", tolower(names(tmp))) & !grepl("manifest", tolower(names(tmp)))]

# Combine multi-part parquet datasets
combine_duplicate_dfs <- function(df_list) {
  df_names <- unique(names(df_list))
  
  for (i in seq_along(df_names)) {
    df_name <- df_names[i]
    df_matches <- grepl(paste0("^", df_name, "$"), names(df_list))
    if (sum(df_matches) > 1) {
      df_combined <- do.call(bind_rows, df_list[df_matches])
      df_list <- c(df_list[!df_matches], list(df_combined))
      names(df_list)[length(df_list)] <- df_name
    }
  }
  return(df_list)
}

unified_df_list <- 
  combine_duplicate_dfs(tmp) %>% 
  lapply(function(x) {
    names(x) <- tolower(names(x))
    return(x)})

df_list <- lapply(unified_df_list, function(df) {
  names(df) <- gsub("value", "value_original", names(df))
  return(df)
})

rm(parent_directory, file_paths, tmp, unified_df_list)

# Data Summarization ----------------------------------------------------------------------------------------------

# 1. Store approved and excluded concepts columns

all_cols <- df_list %>%
  lapply(names) %>%
  unlist() %>%
  enframe() %>%
  mutate(name = gsub("\\s\\d+|\\d", "", name))

approved_concepts_non_summarized <- 
  concept_map$concept_cd %>%
  grep("^(?!.*(?:summary|trigger)).+$", ., perl = T, value = T) %>% 
  str_extract("(?<=:)[^:]*$") %>% 
  str_replace_all(c("mins" = "minutes",
                    "avghr" = "averageheartrate",
                    "spo2(?!_)" = "spo2_",
                    "hrvd" = "hrv_d",
                    "restinghr$" = "restingheartrate",
                    "sleepbrth" = "sleepsummarybreath")) %>% 
  unique()

excluded_concepts_non_summarized <- 
  all_cols$value %>% 
  unique() %>% 
  setdiff(tolower(approved_concepts_non_summarized)) %>% 
  unique()

approved_concepts_summarized <- 
  concept_map$concept_cd %>%
  grep("^(?=.*summary)(?!.*trigger).+$", ., perl = T, value = T) %>% 
  str_extract("(?<=:)[^:]*$") %>% 
  str_replace_all(c("mins" = "minutes",
                    "avghr" = "averageheartrate",
                    "spo2(?!_)" = "spo2_",
                    "hrv" = "hrv_dailyrmssd",
                    "restinghr$" = "restingheartrate",
                    "sleepbrth" = "sleepsummarybreath")) %>% 
  unique()

excluded_concepts_summarized <- 
  all_cols$value %>% 
  unique() %>% 
  setdiff(tolower(approved_concepts_summarized)) %>% 
  unique()

rm(all_cols, approved_concepts_non_summarized, approved_concepts_summarized)

# 2. Melt data frames from wide to long with new concept (variable) and value (variable value) columns

# Define a function to melt a data frame
melt_df <- function(df, excluded_concepts) {
  approved_cols <- setdiff(names(df), excluded_concepts)
  df_melt <- melt(df, 
                  id.vars = intersect(excluded_concepts, names(df)), 
                  measure.vars = approved_cols,
                  variable.name = "concept", 
                  value.name = "value")
  return(df_melt)
}

# Apply the melt_df function to the list of data frames
filtered_df_list_summarized <- 
  df_list %>% 
  lapply(melt_df, excluded_concepts_summarized) %>% 
  lapply(function(x) {
    x %>% 
      select(if("participantidentifier" %in% colnames(x)) "participantidentifier",
             matches("(?<!_)date(?!_)", perl = T),
             if("concept" %in% colnames(x)) "concept",
             if("value" %in% colnames(x)) "value")
  }) %>% 
  {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
  lapply(drop_na, "value")

filtered_df_list_non_summarized <- 
  df_list %>% 
  lapply(melt_df, excluded_concepts_non_summarized) %>% 
  lapply(function(x) {
    x %>% 
      select(if("participantidentifier" %in% colnames(x)) "participantidentifier",
             matches("(?<!_)date(?!_)", perl = T),
             if("concept" %in% colnames(x)) "concept",
             if("value" %in% colnames(x)) "value")
  }) %>% 
  {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
  lapply(drop_na, "value")

rm(df_list)

convert_col_to_numeric <- function(df_list) {
  for (i in seq_along(df_list)) {
    if (!grepl("device", names(df_list)[i])) {
      df_list[[i]]$value <- sapply(df_list[[i]]$value, as.numeric)
    }
  }
  return(df_list)
}

filtered_df_list_non_summarized %<>% convert_col_to_numeric()
filtered_df_list_summarized %<>% convert_col_to_numeric()

rm(excluded_concepts_summarized, excluded_concepts_non_summarized)

# 3. Format non-summarized data as per i2b2 specs

add_i2b2_prefix <- function(dataset) {
  dataset_name <- names(dataset)
  result <- list()

  for (i in seq_along(dataset)) {
    if (grepl("fitbit", dataset_name[i])) {
      dataset_name[i] <- sub("fitbit", "fitbit:", dataset_name[i])
    } else if (grepl("healthkitv2", dataset_name[i])) {
      dataset_name[i] <- sub("healthkitv2", "healthkit:", dataset_name[i])
    } else if (grepl("symptomlog$", dataset_name[i])) {
      dataset_name[i] <- sub("symptomlog", "symptomlog", dataset_name[i])
    } else if (grepl("symptomlog_value_s", dataset_name[i])) {
      dataset_name[i] <- sub("symptomlog_value_symptoms", "symptomlog:symptoms", dataset_name[i])
    } else if (grepl("symptomlog_value_t", dataset_name[i])) {
      dataset_name[i] <- sub("symptomlog_value_treatments", "symptomlog:treatments", dataset_name[i])
    } else {
      dataset_name[i] <- gsub("", "survey:", dataset_name[i])
    }
    
    result[[i]] <-
      dataset[[i]] %>%
      mutate(concept = paste0("mhp:", dataset_name[i], ":", concept))
  }
  
  names(result) <- names(dataset)
  return(result)
}

non_summarized_tmp <- 
  filtered_df_list_non_summarized %>% 
  add_i2b2_prefix() %>% 
  {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
  lapply(function(x) {
    if ("date" %in% colnames(x) & "modifieddate" %in% colnames(x)) {
      x %<>% 
        select(-modifieddate) %>% 
        rename(startdate = date) %>% 
        mutate(enddate = NA) %>% 
        mutate(valtype_cd = case_when(class(value) == "numeric" ~ "N", 
                                      class(value) == "character" ~ "T")) %>% 
        mutate(nval_num = as.numeric(case_when(valtype_cd == "numeric" ~ value)),
               tval_char = as.character(case_when(valtype_cd == "character" ~ value))) %>%
        select(-value)
      return(x)
    } else if ("date" %in% colnames(x) & !"modifieddate" %in% colnames(x)){
      x %<>% 
        rename(startdate = date) %>% 
        mutate(enddate = NA) %>% 
        mutate(valtype_cd = case_when(class(value) == "numeric" ~ "N", 
                                      class(value) == "character" ~ "T")) %>% 
        mutate(nval_num = as.numeric(case_when(valtype_cd == "numeric" ~ value)),
               tval_char = as.character(case_when(valtype_cd == "character" ~ value))) %>%
        select(-value)
      return(x)
    } else if ("startdate" %in% colnames(x) & "enddate" %in% colnames(x)) {
      x %<>% 
        mutate(valtype_cd = case_when(class(value) == "numeric" ~ "N", 
                                      class(value) == "character" ~ "T")) %>% 
        mutate(nval_num = as.numeric(case_when(valtype_cd == "numeric" ~ value)),
               tval_char = as.character(case_when(valtype_cd == "character" ~ value))) %>%
        select(-value)
      return(x)
    }
  }) %>% 
  bind_rows()

rm(filtered_df_list_non_summarized)

# 4. Summarize data on specific time scales (weekly, all-time) for specified statistics (5/95 percentiles, mean, median, variance, number of records)

summary <- function(dataset) {
  
  if ("startdate" %in% colnames(dataset) & "enddate" %in% colnames(dataset)) {
    summarize_stat <- function(dataset, stat, timescale) {
      dataset %>%
        select(participantidentifier, startdate, enddate, concept, value) %>%
        group_by(participantidentifier, concept) %>%
        mutate("stat_value" = switch(stat,
                                     "5pct" = quantile(as.numeric(value), 0.05, na.rm = T),
                                     "95pct" = quantile(as.numeric(value), 0.95, na.rm = T),
                                     "mean" = mean(as.numeric(value), na.rm = T),
                                     "median" = median(as.numeric(value), na.rm = T),
                                     "variance" = var(as.numeric(value), na.rm = T),
                                     "numrecords" = n()
        )
        ) %>%
        select(-value) %>%
        rename(value = stat_value) %>%
        mutate(
          startdate = as_date(min(startdate)),
          enddate = as_date(max(enddate)),
          timescale = timescale,
          stat = stat,
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
    }
    
    summarize_weekly <- function(dataset, stat, timescale) {
      dataset %>%
        select(participantidentifier, concept, value, startdate) %>%
        mutate(
          startdate = as_date(startdate),
          year = year(startdate),
          week = week(startdate)
        ) %>%
        filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
        group_by(participantidentifier, concept, year, week) %>%
        summarise("value" = switch(stat,
                                   "5pct" = quantile(as.numeric(value), 0.05, na.rm = T),
                                   "95pct" = quantile(as.numeric(value), 0.95, na.rm = T),
                                   "mean" = mean(as.numeric(value), na.rm = T),
                                   "median" = median(as.numeric(value), na.rm = T),
                                   "variance" = var(as.numeric(value), na.rm = T),
                                   "numrecords" = n()
        ),
        .groups = "keep") %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            (make_date(year, 1, 1) + weeks(week-1)) %>% floor_date(unit = "week", week_start = "Sunday"),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = timescale,
          stat = stat,
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        select(-c(year, week)) %>%
        select(
          participantidentifier,
          week_summary_start_date,
          week_summary_end_date,
          concept,
          value
        ) %>%
        rename(startdate = week_summary_start_date) %>%
        rename(enddate = week_summary_end_date)
    }
    
  } else {
    if ("date" %in% colnames(dataset)) {
      summarize_stat <- function(dataset, stat, timescale) {
        dataset %>%
          select(participantidentifier, date, concept, value) %>%
          group_by(participantidentifier, concept) %>%
          mutate("stat_value" = switch(stat,
                                       "5pct" = quantile(as.numeric(value), 0.05, na.rm = T),
                                       "95pct" = quantile(as.numeric(value), 0.95, na.rm = T),
                                       "mean" = mean(as.numeric(value), na.rm = T),
                                       "median" = median(as.numeric(value), na.rm = T),
                                       "variance" = var(as.numeric(value), na.rm = T),
                                       "numrecords" = n()
          )
          ) %>%
          select(-value) %>%
          rename(value = stat_value) %>%
          rename(startdate = date) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = timescale,
            stat = stat,
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
      }
      
      summarize_weekly <- function(dataset, stat, timescale) {
        dataset %>%
          select(participantidentifier, concept, value, date) %>%
          rename(startdate = date) %>% 
          mutate(
            startdate = as_date(startdate),
            year = year(startdate),
            week = week(startdate)
          ) %>%
          filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
          group_by(participantidentifier, concept, year, week) %>%
          summarise("value" = switch(stat,
                                     "5pct" = quantile(as.numeric(value), 0.05, na.rm = T),
                                     "95pct" = quantile(as.numeric(value), 0.95, na.rm = T),
                                     "mean" = mean(as.numeric(value), na.rm = T),
                                     "median" = median(as.numeric(value), na.rm = T),
                                     "variance" = var(as.numeric(value), na.rm = T),
                                     "numrecords" = n()
          ),
          .groups = "keep") %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              (make_date(year, 1, 1) + weeks(week-1)) %>% floor_date(unit = "week", week_start = "Sunday"),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = timescale,
            stat = stat,
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          select(-c(year, week)) %>%
          select(
            participantidentifier,
            week_summary_start_date,
            week_summary_end_date,
            concept,
            value
          ) %>%
          rename(startdate = week_summary_start_date) %>%
          rename(enddate = week_summary_end_date)
      }
      
    } else {
      if ("datetime" %in% colnames(dataset) & !"date" %in% colnames(dataset)) {
        summarize_stat <- function(dataset, stat, timescale) {
          dataset %>%
            select(participantidentifier, datetime, concept, value) %>%
            group_by(participantidentifier, concept) %>%
            mutate("stat_value" = switch(stat,
                                         "5pct" = quantile(as.numeric(value), 0.05, na.rm = T),
                                         "95pct" = quantile(as.numeric(value), 0.95, na.rm = T),
                                         "mean" = mean(as.numeric(value), na.rm = T),
                                         "median" = median(as.numeric(value), na.rm = T),
                                         "variance" = var(as.numeric(value), na.rm = T),
                                         "numrecords" = n()
            )
            ) %>%
            select(-value) %>%
            rename(value = stat_value) %>% 
            rename(startdate = datetime) %>% 
            mutate(
              startdate = as_date(min(startdate)),
              enddate = NA,
              timescale = timescale,
              stat = stat,
              concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
            ) %>%
            distinct() %>%
            ungroup() %>%
            select(-c(timescale, stat))
        }
        
        summarize_weekly <- function(dataset, stat, timescale) {
          dataset %>%
            select(participantidentifier, concept, value, datetime) %>%
            rename(startdate = datetime) %>% 
            mutate(
              startdate = as_date(startdate),
              year = year(startdate),
              week = week(startdate)
            ) %>%
            filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
            group_by(participantidentifier, concept, year, week) %>%
            summarise("value" = switch(stat,
                                       "5pct" = quantile(as.numeric(value), 0.05, na.rm = T),
                                       "95pct" = quantile(as.numeric(value), 0.95, na.rm = T),
                                       "mean" = mean(as.numeric(value), na.rm = T),
                                       "median" = median(as.numeric(value), na.rm = T),
                                       "variance" = var(as.numeric(value), na.rm = T),
                                       "numrecords" = n()
            ),
            .groups = "keep") %>%
            ungroup() %>%
            mutate(
              week_summary_start_date =
                (make_date(year, 1, 1) + weeks(week-1)) %>% floor_date(unit = "week", week_start = "Sunday"),
              week_summary_end_date =
                week_summary_start_date + days(6),
              timescale = timescale,
              stat = stat,
              concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
            ) %>%
            select(-c(year, week)) %>%
            select(
              participantidentifier,
              week_summary_start_date,
              week_summary_end_date,
              concept,
              value
            ) %>%
            rename(startdate = week_summary_start_date) %>%
            rename(enddate = week_summary_end_date)
        }
      } else {
        stop("Error: date column not found")
      }
    }
  }
  
  all_pct5 <- summarize_stat(dataset, "5pct", "alltime")
  all_pct95 <- summarize_stat(dataset, "95pct", "alltime")
  all_mean <- summarize_stat(dataset, "mean", "alltime")
  all_median <- summarize_stat(dataset, "median", "alltime")
  all_variance <- summarize_stat(dataset, "variance", "alltime")
  all_numrecords <- summarize_stat(drop_na(dataset), "numrecords", "alltime")
  
  weekly_pct5 <- summarize_weekly(dataset, "5pct", "weekly")
  weekly_pct95 <- summarize_weekly(dataset, "95pct", "weekly")
  weekly_mean <- summarize_weekly(dataset, "mean", "weekly")
  weekly_median <- summarize_weekly(dataset, "median", "weekly")
  weekly_variance <- summarize_weekly(dataset, "variance", "weekly")
  weekly_numrecords <- summarize_weekly(drop_na(dataset), "numrecords", "weekly")
  
  
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

# Filter the list to only include data frames with "concept" column and apply the summary function on the filtered list
summarized_tmp <- 
  filtered_df_list_summarized %>% 
  {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
  lapply(summary) %>% 
  bind_rows()

rm(filtered_df_list_summarized)

# 5. Update output to match concept map format

process_df <- function(df) {
  if (any(grepl("summary", df$concept))) {
    df$concept %<>% 
      tolower() %>% 
      {gsub("sleepsummarybreath", "sleepbrth", .)} %>% 
      {gsub("restingheartrate", "restinghr", ., perl = T)} %>% 
      {gsub("hrv_dailyrmssd", "hrv", .)} %>% 
      {gsub("spo2_", "spo2", ., perl = T)} %>% 
      {gsub("averageheartrate", "avghr", .)} %>% 
      {gsub("minutes", "mins", .)}
  } else if (any(grepl("mhp:fitbit", df$concept))) {
    df$concept %<>% 
      tolower() %>% 
      {gsub("hrv_d", "hrvd", .)} %>% 
      {gsub("spo2_", "spo2", ., perl = T)} %>% 
      {gsub("minutes", "mins", .)}
  }
  
  if (all((c("participantidentifier", "startdate", "enddate", "concept", "value") %in% colnames(df)))) {
    if (all(c("valtype_cd", "nval_num", "tval_char") %in% colnames(df))) {
      df %<>% 
        mutate(startdate = as_date(startdate),
               enddate = as_date(enddate)) %>%
        left_join(select(concept_map, concept_cd, UNITS_CD),
                  by = c("concept" = "concept_cd")) %>% 
        drop_na(valtype_cd)
    } else {
      df %<>%
        mutate(startdate = as_date(startdate),
               enddate = as_date(enddate)) %>%
        mutate(valtype_cd = case_when(class(value) == "numeric" ~ "N", 
                                      class(value) == "character" ~ "T")) %>%
        mutate(nval_num = as.numeric(case_when(valtype_cd == "numeric" ~ value)),
               tval_char = as.character(case_when(valtype_cd == "character" ~ value))) %>%
        select(-value) %>%
        left_join(select(concept_map, concept_cd, UNITS_CD),
                  by = c("concept" = "concept_cd")) %>% 
        drop_na(valtype_cd)
    }
  } else {
    stop("Error: One or all of {parcipantidentifier, startdate, enddate, concept, value} columns not found")
  }
  colnames(df) <- tolower(colnames(df))
  return(df)
}

summarized_tmp2 <- map_if(summarized_tmp, ~"concept" %in% names(.), process_df) %>% as.data.frame()
non_summarized_tmp2 <- map_if(non_summarized_tmp, ~"concept" %in% names(.), ~process_df(.) %>% filter(concept %in% concept_map$concept_cd)) %>% as.data.frame()

output_concepts <- 
  bind_rows(summarized_tmp2, non_summarized_tmp2) %>% 
  arrange(concept) %>% 
  mutate(across(.fns = as.character)) %>% 
  replace(is.na(.), "<null>")

rm(summarized_tmp, non_summarized_tmp, summarized_tmp2, non_summarized_tmp2)

# TODO: automate export to csv>synapse

# Export output
-----------------------------------------------------------------------------------------------

# 1. Write to CSV

if (dir.exists("deliverables")) {
  write.csv(output_concepts, file = 'deliverables/output_concepts.csv', row.names = F)
  write.csv(concept_map, file = 'deliverables/concepts_map.csv', row.names = F)
} else {
  dir.create("deliverables")
  write.csv(output_concepts, file = 'deliverables/output_concepts.csv', row.names = F)
  write.csv(concept_map, file = 'deliverables/concepts_map.csv', row.names = F)
}

# 2. Export to Synapse
folder.synid <- "syn51184127"

tmp <- synapser::File(path = 'deliverables/output_concepts.csv', parent = folder.synid, name = "output_concepts.csv")
tmp <- synapser::synStore(tmp)

tmp <- synapser::File(path = 'deliverables/concepts_map.csv', parent = folder.synid, name = "concepts_map.csv",)
tmp <- synapser::synStore(tmp)

