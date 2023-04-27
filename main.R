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

# Get i2b2 concepts map ---------------------------------------------------

ontologyFileID <- "syn51320791"

get_concept_map <- function(synID) {
  df <- 
    synGet(synID) %>% 
    {read.csv(.$path)} %>% 
    mutate(concept_cd = tolower(concept_cd))
  
  return(df)
}

concept_map <- get_concept_map(ontologyFileID)

# Read parquet files to df ------------------------------------------------
parquetDirID <- "syn50996868"

synget_parquet_to_df <- function(synDirID, name_filter) {
  system(paste("synapse get -r", synDirID))
  
  file_paths <- list.files(recursive = T, full.names = T)
  file_paths <- file_paths[grepl("dataset_", (file_paths), ignore.case = T)]
  
  df_list <- lapply(file_paths, function(file_path) {
    if (grepl(".parquet$", file_path)) {
      read_parquet(file_path)
    } else if (grepl(".tsv$", file_path)) {
      read_tsv(file_path, show_col_types = F)
    } else if (grepl(".ndjson$", file_path)) {
      ndjson::stream_in(file_path, cls = "tbl")
    } else if (grepl(".csv$", file_path)) {
      read.csv(file_path)
    } else {
      stop(paste("Unsupported file format for", file_path))
    }
  })
  
  names(df_list) <- 
    file_paths %>% 
    {paste(basename(dirname(.)), "-", basename(.))} %>% 
    {gsub("\\.(parquet|tsv|ndjson)$|(dataset_|-.*\\.snappy| )", "", .)}
  
  df_list <- df_list[grepl(name_filter, tolower(names(df_list))) & !grepl("manifest", tolower(names(df_list)))]
  
  return(df_list)
}

df_list_original <- synget_parquet_to_df(synDirID = parquetDirID)

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

df_list_unified_tmp <- 
  combine_duplicate_dfs(df_list_original) %>% 
  lapply(function(x) {
    names(x) <- tolower(names(x))
    return(x)})

df_list <- 
  df_list_unified_tmp %>% 
  lapply(function(df) {
    names(df) <- gsub("value", "value_original", names(df))
    return(df)
  })

rm(df_list_original, df_list_unified_tmp)

# Data Summarization ----------------------------------------------------------------------------------------------

# 1. Store approved and excluded concepts columns
concept_replacements <- c("mins" = "minutes",
                          "avghr" = "averageheartrate",
                          "spo2" = "spo2_",
                          "hrv" = "hrv_dailyrmssd",
                          "restinghr" = "restingheartrate",
                          "sleepbrth" = "sleepsummarybreath")

reverse_str_pairs <- function(str_pairs) {
  reversed <- setNames(names(str_pairs), str_pairs)
  reversed <- rev(reversed)
  return(reversed)
}

concept_replacements_reversed <- reverse_str_pairs(concept_replacements)

diff_concepts <- function(df_list, concept_replacements, concept_map) {
  all_cols <- df_list %>%
    lapply(names) %>%
    unlist() %>%
    enframe() %>%
    mutate(name = gsub("\\s\\d+|\\d", "", name))
  
  approved_concepts_summarized <- 
    concept_map$concept_cd %>%
    grep("^(?=.*summary)(?!.*trigger).+$", ., perl = T, value = T) %>% 
    str_extract("(?<=:)[^:]*$") %>% 
    str_replace_all(concept_replacements) %>% 
    unique()
  
  excluded_concepts <- 
    all_cols$value %>% 
    unique() %>% 
    setdiff(tolower(approved_concepts_summarized)) %>% 
    unique()
  
  return(excluded_concepts)
}

excluded_concepts <- diff_concepts(df_list = df_list, concept_replacements = concept_replacements, concept_map = concept_map)

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
df_list_melted_filtered <- 
  df_list %>% 
  lapply(melt_df, excluded_concepts) %>% 
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

convert_col_to_numeric <- function(df_list, df_to_avoid = "device", col_to_convert = "value") {
  for (i in seq_along(df_list)) {
    if (!grepl(df_to_avoid, names(df_list)[i])) {
      df_list[[i]][[col_to_convert]] <- sapply(df_list[[i]][[col_to_convert]], as.numeric)
    }
  }
  return(df_list)
}

df_list_melted_filtered %<>% convert_col_to_numeric()

rm(excluded_concepts)

# 3. Summarize data on specific time scales (weekly, all-time) for specified statistics (5/95 percentiles, mean, median, variance, number of records)
stat_summarize <- function(df) {
  
  summarize_stat_date <- function(df, timescale) {
    if ("startdate" %in% colnames(df) & "enddate" %in% colnames(df)) {
      # Do nothing
    } else if ("date" %in% colnames(df)) {
      df %<>% 
        rename(startdate = date) %>% 
        mutate(enddate = NA)
    } else if ("datetime" %in% colnames(df) & !"date" %in% colnames(df)) {
      df %<>% 
        rename(startdate = datetime) %>% 
        mutate(enddate = NA)
    } else {
      stop("Error: No 'date' column found")
    }
    
    df %>%
      select(participantidentifier, startdate, enddate, concept, value) %>%
      group_by(participantidentifier, concept) %>%
      summarize(startdate = as_date(min(startdate)),
                enddate = as_date(max(enddate)),
                mean = mean(as.numeric(value), na.rm = T),
                median = median(as.numeric(value), na.rm = T),
                variance = var(as.numeric(value), na.rm = T),
                `5pct` = quantile(as.numeric(value), 0.05, na.rm = T),
                `95pct` = quantile(as.numeric(value), 0.95, na.rm = T),
                numrecords = n(),
                .groups = "keep") %>%
      ungroup() %>% 
      pivot_longer(cols = c(mean, median, variance, `5pct`, `95pct`, numrecords),
                   names_to = "stat",
                   values_to = "value") %>%
      mutate(concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)) %>%
      select(participantidentifier, startdate, enddate, concept, value) %>%
      distinct()
  }
  
  
  summarize_weekly_date <- function(df, timescale) {
    if ("startdate" %in% colnames(df)) {
      # Do nothing
    } else if ("date" %in% colnames(df)) {
      df %<>% 
        rename(startdate = date)
    } else if ("datetime" %in% colnames(df) & !"date" %in% colnames(df)) {
      df %<>% 
        rename(startdate = datetime)
    } else {
      stop("Error: No 'date' column found")
    }
    
    df %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = 7)) %>%
      group_by(participantidentifier, concept, year, week) %>%
      summarise(`5pct` = quantile(as.numeric(value), 0.05, na.rm = T),
                `95pct` = quantile(as.numeric(value), 0.95, na.rm = T),
                mean = mean(as.numeric(value), na.rm = T),
                median = median(as.numeric(value), na.rm = T),
                variance = var(as.numeric(value), na.rm = T),
                numrecords = n(),
                startdate =
                  (make_date(year, 1, 1) + weeks(week-1)) %>% floor_date(unit = "week", week_start = 7),
                enddate =
                  startdate + days(6),
                .groups = "keep") %>%
      ungroup() %>%
      pivot_longer(cols = c(mean, median, variance, `5pct`, `95pct`, numrecords),
                   names_to = "stat",
                   values_to = "value") %>%
      mutate(concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)) %>%
      select(participantidentifier, startdate, enddate, concept, value) %>% 
      distinct()
  }
  
  result <- 
    bind_rows(summarize_stat_date(df, "alltime"), 
              summarize_weekly_date(df, "weekly")) %>% 
    distinct()
  
  return(result)
}

# Filter the list to only include data frames with "concept" column and apply the summary function on the filtered list
df_summarized <- 
  df_list_melted_filtered %>% 
  {Filter(function(df) "concept" %in% colnames(df), .)} %>% 
  lapply(stat_summarize) %>% 
  bind_rows() %>% 
  distinct()

rm(df_list_melted_filtered)

# 4. Update output to match concept map format
process_df <- function(df, concept_map, concept_replacements_reversed) {
  if (any(grepl("summary", df$concept))) {
    df$concept %<>% 
      tolower() %>% 
      str_replace_all(concept_replacements_reversed)
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
  process_df(df_summarized, concept_map, concept_replacements_reversed) %>% 
  mutate(nval_num = signif(nval_num, 9)) %>% 
  arrange(concept) %>% 
  mutate(across(.fns = as.character)) %>% 
  replace(is.na(.), "<null>") %>% 
  filter(nval_num != "<null>" | tval_char != "<null>")

rm(df_summarized, concept_replacements, concept_replacements_reversed)

# Export output ---------------------------------------------------------------------------------------------------

# 1. Write to CSV
write.csv(output_concepts, file = 'output_concepts.csv', row.names = F)
write.csv(concept_map, file = 'concepts_map.csv', row.names = F)

# 2. Export to Synapse
store_in_syn <- function(synFolderID, filepath, used_param = NULL, executed_param = NULL) {
  fileObj <- synapser::File(path = filepath, parent = synFolderID, name = filepath)
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

synFolderID <- "syn51184127"
store_in_syn(synFolderID, 'output_concepts.csv', used_param = ontologyFileID, executed_param = "https://github.com/Sage-Bionetworks/recoverSummarizeR/blob/main/main.R")
store_in_syn(synFolderID, 'concepts_map.csv', used_param = ontologyFileID)
rm(synFolderID, ontologyFileID)

