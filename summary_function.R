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

