# Import Libraries --------------------------------------------------------

library(synapser)
library(magrittr)
library(dplyr)
library(tidyr)
library(tibble)
library(jsonlite)
library(stringr)
library(arrow)
library(readr)
library(reshape2)
library(lubridate)
library(purrr)

# Get data ----------------------------------------------------------------

synLogin()
parquet.dir.id <- "syn50996868"
system(paste("synapse get -r", parquet.dir.id))
rm(parquet.dir.id)

# Get i2b2 concepts map ---------------------------------------------------

ontology.file.id <- "syn51320791"
tmpObj <- synGet(ontology.file.id)
concept_map <- read.csv(tmpObj$path)
concept_map$concept_cd %<>% tolower()
rm(tmpObj)

# Read parquet files to df ------------------------------------------------

file_paths <- list.files(recursive = T, full.names = T)
file_paths <- file_paths[grepl("dataset_", (file_paths), ignore.case = T)]

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

rm(file_paths, tmp, unified_df_list)

# Data Summarization ----------------------------------------------------------------------------------------------

# 1. Store approved and excluded concepts columns
all_cols <- df_list %>%
  lapply(names) %>%
  unlist() %>%
  enframe() %>%
  mutate(name = gsub("\\s\\d+|\\d", "", name))

str_replacements <- c("mins" = "minutes",
                      "avghr" = "averageheartrate",
                      "spo2" = "spo2_",
                      "hrv" = "hrv_dailyrmssd",
                      "restinghr" = "restingheartrate",
                      "sleepbrth" = "sleepsummarybreath")

str_replacements_rev <- setNames(names(str_replacements), str_replacements)
str_replacements_rev <- rev(str_replacements_rev)

approved_concepts_summarized <- 
  concept_map$concept_cd %>%
  grep("^(?=.*summary)(?!.*trigger).+$", ., perl = T, value = T) %>% 
  str_extract("(?<=:)[^:]*$") %>% 
  str_replace_all(str_replacements) %>% 
  unique()

excluded_concepts_summarized <- 
  all_cols$value %>% 
  unique() %>% 
  setdiff(tolower(approved_concepts_summarized)) %>% 
  unique()

rm(all_cols, approved_concepts_summarized)

# 2. Melt data frames from wide to long with new concept (variable) and value (variable value) columns
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

rm(df_list)

convert_col_to_numeric <- function(df_list) {
  for (i in seq_along(df_list)) {
    if (!grepl("device", names(df_list)[i])) {
      df_list[[i]]$value <- sapply(df_list[[i]]$value, as.numeric)
    }
  }
  return(df_list)
}

filtered_df_list_summarized %<>% convert_col_to_numeric()

rm(excluded_concepts_summarized)

# 3. Summarize data on specific time scales (weekly, all-time) for specified statistics (5/95 percentiles, mean, median, variance, number of records)
summary <- function(dataset) {
  
  summarize_stat_date <- function(dataset, stat, timescale) {
    if ("startdate" %in% colnames(dataset) & "enddate" %in% colnames(dataset)) {
      # Do nothing
    } else if ("date" %in% colnames(dataset)) {
      dataset %<>% 
        rename(startdate = date) %>% 
        mutate(enddate = NA)
    } else if ("datetime" %in% colnames(dataset) & !"date" %in% colnames(dataset)) {
      dataset %<>% 
        rename(startdate = datetime) %>% 
        mutate(enddate = NA)
    } else {
      stop("Error: No 'date' column found")
    }
    
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
  
  summarize_weekly_date <- function(dataset, stat, timescale) {
    if ("startdate" %in% colnames(dataset)) {
      # Do nothing
    } else if ("date" %in% colnames(dataset)) {
      dataset %<>% 
        rename(startdate = date)
    } else if ("datetime" %in% colnames(dataset) & !"date" %in% colnames(dataset)) {
      dataset %<>% 
        rename(startdate = datetime)
    } else {
      stop("Error: No 'date' column found")
    }
    
    dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = 7)) %>%
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
          (make_date(year, 1, 1) + weeks(week-1)) %>% floor_date(unit = "week", week_start = 7),
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
  
  all_pct5 <- summarize_stat_date(dataset, "5pct", "alltime")
  all_pct95 <- summarize_stat_date(dataset, "95pct", "alltime")
  all_mean <- summarize_stat_date(dataset, "mean", "alltime")
  all_median <- summarize_stat_date(dataset, "median", "alltime")
  all_variance <- summarize_stat_date(dataset, "variance", "alltime")
  all_numrecords <- summarize_stat_date(drop_na(dataset), "numrecords", "alltime")

  weekly_pct5 <- summarize_weekly_date(dataset, "5pct", "weekly")
  weekly_pct95 <- summarize_weekly_date(dataset, "95pct", "weekly")
  weekly_mean <- summarize_weekly_date(dataset, "mean", "weekly")
  weekly_median <- summarize_weekly_date(dataset, "median", "weekly")
  weekly_variance <- summarize_weekly_date(dataset, "variance", "weekly")
  weekly_numrecords <- summarize_weekly_date(drop_na(dataset), "numrecords", "weekly")
  
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

# 4. Update output to match concept map format
process_df <- function(df) {
  if (any(grepl("summary", df$concept))) {
    df$concept %<>% 
      tolower() %>% 
      str_replace_all(str_replacements_rev)
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
        mutate(nval_num = as.numeric(case_when(valtype_cd == "N" ~ value)),
               tval_char = as.character(case_when(valtype_cd == "T" ~ value))) %>%
        select(-value) %>%
        left_join(select(concept_map, concept_cd, UNITS_CD),
                  by = c("concept" = "concept_cd")) %>% 
        drop_na(valtype_cd)
    }
  } else {
    stop("Error: One or all of {participantidentifier, startdate, enddate, concept, value} columns not found")
  }
  colnames(df) <- tolower(colnames(df))
  return(df)
}

output_concepts <- 
  process_df(summarized_tmp) %>% 
  mutate(nval_num = signif(nval_num, 9)) %>% 
  arrange(concept) %>% 
  mutate(across(.fns = as.character)) %>% 
  replace(is.na(.), "<null>") %>% 
  filter(nval_num != "<null>" | tval_char != "<null>")

rm(summarized_tmp, str_replacements, str_replacements_rev)

# Export output ---------------------------------------------------------------------------------------------------

# 1. Write to CSV
write.csv(output_concepts, file = 'output_concepts.csv', row.names = F)
write.csv(concept_map, file = 'concepts_map.csv', row.names = F)

# 2. Export to Synapse
store_in_syn_dir <- function(synfolder.id, fileEnt, used_param = NULL, executed_param = NULL) {
  fileObj <- synapser::File(path = fileEnt, parent = synfolder.id, name = fileEnt)
  if (!is.null(used_param) && !is.null(executed_param)) {
    fileObj <- synapser::synStore(fileObj, used = used_param, executed = executed_param)
  } else if (!is.null(used_param)) {
    fileObj <- synapser::synStore(fileObj, used = used_param)
  } else if (!is.null(executed_param)) {
    fileObj <- synapser::synStore(fileObj, executed = executed_param)
  } else {
    fileObj <- synapser::synStore(fileObj)
  }
  return(fileObj)
}

synfolder.id <- "syn51184127"
store_in_syn_dir(synfolder.id, 'output_concepts.csv', used_param = ontology.file.id, executed_param = "https://github.com/pranavanba/convert2i2b2/blob/main/main.R")
store_in_syn_dir(synfolder.id, 'concepts_map.csv', used_param = ontology.file.id)
rm(synfolder.id, ontology.file.id)

