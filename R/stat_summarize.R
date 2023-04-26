# This function takes a data frame as input and summarizes the data on specific time scales (weekly, all-time) for the following statistics (5/95 percentiles, mean, median, variance, number of records).
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
