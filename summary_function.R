summary <- function(dataset) {
  
  if ("startdate" %in% colnames(dataset) & "enddate" %in% colnames(dataset)) {
    all_pct5 <- 
      dataset %>%
      select(participantidentifier, startdate, enddate, concept, value,) %>%
      group_by(participantidentifier, concept) %>%
      mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
      select(-c(value)) %>%
      rename(value = quantile) %>%
      mutate(
        startdate = as_date(min(startdate)),
        enddate = as_date(max(enddate)),
        timescale = "alltime",
        stat = "5pct",
        concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
      ) %>%
      distinct() %>%
      ungroup() %>%
      select(-c(timescale, stat))
    
    all_pct95 <- 
      dataset %>%
      select(participantidentifier, startdate, enddate, concept, value,) %>%
      group_by(participantidentifier, concept) %>%
      mutate("quantile" = quantile(as.numeric(value), 0.95, na.rm = T)) %>%
      select(-c(value)) %>%
      rename(value = quantile) %>%
      mutate(
        startdate = as_date(min(startdate)),
        enddate = as_date(max(enddate)),
        timescale = "alltime",
        stat = "95pct",
        concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
      ) %>%
      distinct() %>%
      ungroup() %>%
      select(-c(timescale, stat))
    
    all_mean <- 
      dataset %>%
      select(participantidentifier, startdate, enddate, concept, value,) %>%
      group_by(participantidentifier, concept) %>%
      mutate("mean" = mean(as.numeric(value), na.rm = T)) %>%
      select(-c(value)) %>%
      rename(value = mean) %>%
      mutate(
        startdate = as_date(min(startdate)),
        enddate = as_date(max(enddate)),
        timescale = "alltime",
        stat = "mean",
        concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
      ) %>%
      distinct() %>%
      ungroup() %>%
      select(-c(timescale, stat))
    
    all_median <- 
      dataset %>%
      select(participantidentifier, startdate, enddate, concept, value,) %>%
      group_by(participantidentifier, concept) %>%
      mutate("median" = median(as.numeric(value), na.rm = T)) %>%
      select(-c(value)) %>%
      rename(value = median) %>%
      mutate(
        startdate = as_date(min(startdate)),
        enddate = as_date(max(enddate)),
        timescale = "alltime",
        stat = "median",
        concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
      ) %>%
      distinct() %>%
      ungroup() %>%
      select(-c(timescale, stat))
    
    all_variance <- 
      dataset %>%
      select(participantidentifier, startdate, enddate, concept, value,) %>%
      group_by(participantidentifier, concept) %>%
      mutate("variance" = var(as.numeric(value), na.rm = T)) %>%
      select(-c(value)) %>%
      rename(value = variance) %>%
      mutate(
        startdate = as_date(min(startdate)),
        enddate = as_date(max(enddate)),
        timescale = "alltime",
        stat = "variance",
        concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
      ) %>%
      distinct() %>%
      ungroup() %>%
      select(-c(timescale, stat))
    
    all_numrecords <- 
      dataset %>%
      select(participantidentifier, startdate, enddate, concept, value,) %>%
      group_by(participantidentifier, concept) %>%
      drop_na() %>%
      add_count() %>%
      select(-c(value)) %>%
      rename(value = n) %>%
      mutate(
        startdate = as_date(min(startdate)),
        enddate = as_date(max(enddate)),
        timescale = "alltime",
        stat = "numrecords",
        concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
      ) %>%
      distinct() %>%
      ungroup() %>%
      select(-c(timescale, stat))
    
    weekly_pct5 <- 
      dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
      group_by(participantidentifier, concept, year, week) %>%
      summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T), .groups = "keep") %>%
      ungroup() %>%
      mutate(
        week_summary_start_date =
          startdate - wday(startdate) + days(1),
        week_summary_end_date =
          week_summary_start_date + days(6),
        timescale = "weekly",
        stat = "5pct",
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
    
    weekly_pct95 <- 
      dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
      group_by(participantidentifier, concept, year, week) %>%
      summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T), .groups = "keep") %>%
      ungroup() %>%
      mutate(
        week_summary_start_date =
          startdate - wday(startdate) + days(1),
        week_summary_end_date =
          week_summary_start_date + days(6),
        timescale = "weekly",
        stat = "95pct",
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
    
    weekly_mean <- 
      dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
      group_by(participantidentifier, concept, year, week) %>%
      summarise("value" = mean(as.numeric(value), na.rm = T), .groups = "keep") %>%
      ungroup() %>%
      mutate(
        week_summary_start_date =
          startdate - wday(startdate) + days(1),
        week_summary_end_date =
          week_summary_start_date + days(6),
        timescale = "weekly",
        stat = "mean",
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
    
    weekly_median <- 
      dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
      group_by(participantidentifier, concept, year, week) %>%
      summarise("value" = median(as.numeric(value), na.rm = T), .groups = "keep") %>%
      ungroup() %>%
      mutate(
        week_summary_start_date =
          startdate - wday(startdate) + days(1),
        week_summary_end_date =
          week_summary_start_date + days(6),
        timescale = "weekly",
        stat = "median",
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
    
    weekly_variance <- 
      dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
      group_by(participantidentifier, concept, year, week) %>%
      summarise("value" = var(as.numeric(value), na.rm = T), .groups = "keep") %>%
      ungroup() %>%
      mutate(
        week_summary_start_date =
          startdate - wday(startdate) + days(1),
        week_summary_end_date =
          week_summary_start_date + days(6),
        timescale = "weekly",
        stat = "variance",
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
    
    weekly_numrecords <-
      dataset %>%
      select(participantidentifier, concept, value, startdate) %>%
      mutate(
        startdate = as_date(startdate),
        year = year(startdate),
        week = week(startdate)
      ) %>%
      filter(startdate >= floor_date(min(startdate), unit = "week", week_start = "Sunday")) %>%
      group_by(participantidentifier, concept, year, week) %>%
      drop_na() %>%
      count() %>%
      rename(value = n) %>%
      ungroup() %>%
      mutate(
        week_summary_start_date =
          startdate - wday(startdate) + days(1),
        week_summary_end_date =
          week_summary_start_date + days(6),
        timescale = "weekly",
        stat = "numrecords",
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
  } else {
    if ("date" %in% colnames(dataset)) {
      all_pct5 <- 
        dataset %>%
        select(participantidentifier, date, concept, value,) %>%
        group_by(participantidentifier, concept) %>%
        mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = quantile) %>%
        rename(startdate = date) %>% 
        mutate(
          startdate = as_date(min(startdate)),
          enddate = NA,
          timescale = "alltime",
          stat = "5pct",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_pct95 <- 
        dataset %>%
        select(participantidentifier, date, concept, value,) %>%
        group_by(participantidentifier, concept) %>%
        mutate("quantile" = quantile(as.numeric(value), 0.95, na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = quantile) %>%
        rename(startdate = date) %>% 
        mutate(
          startdate = as_date(min(startdate)),
          enddate = NA,
          timescale = "alltime",
          stat = "95pct",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_mean <- 
        dataset %>%
        select(participantidentifier, date, concept, value,) %>%
        group_by(participantidentifier, concept) %>%
        mutate("mean" = mean(as.numeric(value), na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = mean) %>%
        rename(startdate = date) %>% 
        mutate(
          startdate = as_date(min(startdate)),
          enddate = NA,
          timescale = "alltime",
          stat = "mean",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_median <- 
        dataset %>%
        select(participantidentifier, date, concept, value,) %>%
        group_by(participantidentifier, concept) %>%
        mutate("median" = median(as.numeric(value), na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = median) %>%
        rename(startdate = date) %>% 
        mutate(
          startdate = as_date(min(startdate)),
          enddate = NA,
          timescale = "alltime",
          stat = "median",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_variance <- 
        dataset %>%
        select(participantidentifier, date, concept, value,) %>%
        group_by(participantidentifier, concept) %>%
        mutate("variance" = var(as.numeric(value), na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = variance) %>%
        rename(startdate = date) %>% 
        mutate(
          startdate = as_date(min(startdate)),
          enddate = NA,
          timescale = "alltime",
          stat = "variance",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_numrecords <- 
        dataset %>%
        select(participantidentifier, date, concept, value,) %>%
        group_by(participantidentifier, concept) %>%
        drop_na() %>%
        add_count() %>%
        select(-c(value)) %>%
        rename(value = n) %>%
        rename(startdate = date) %>% 
        mutate(
          startdate = as_date(min(startdate)),
          enddate = NA,
          timescale = "alltime",
          stat = "numrecords",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      weekly_pct5 <- 
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
        summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T), .groups = "keep") %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            startdate - wday(startdate) + days(1),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = "weekly",
          stat = "5pct",
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
      
      weekly_pct95 <- 
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
        summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T), .groups = "keep") %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            startdate - wday(startdate) + days(1),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = "weekly",
          stat = "95pct",
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
      
      weekly_mean <- 
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
        summarise("value" = mean(as.numeric(value), na.rm = T), .groups = "keep") %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            startdate - wday(startdate) + days(1),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = "weekly",
          stat = "mean",
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
      
      weekly_median <- 
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
        summarise("value" = median(as.numeric(value), na.rm = T), .groups = "keep") %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            startdate - wday(startdate) + days(1),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = "weekly",
          stat = "median",
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
      
      weekly_variance <- 
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
        summarise("value" = var(as.numeric(value), na.rm = T), .groups = "keep") %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            startdate - wday(startdate) + days(1),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = "weekly",
          stat = "variance",
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
      
      weekly_numrecords <-
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
        drop_na() %>%
        count() %>%
        rename(value = n) %>%
        ungroup() %>%
        mutate(
          week_summary_start_date =
            startdate - wday(startdate) + days(1),
          week_summary_end_date =
            week_summary_start_date + days(6),
          timescale = "weekly",
          stat = "numrecords",
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
    } else {
      if ("datetime" %in% colnames(dataset) & !"date" %in% colnames(dataset)) {
        all_pct5 <- 
          dataset %>%
          select(participantidentifier, datetime, concept, value,) %>%
          group_by(participantidentifier, concept) %>%
          mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = quantile) %>%
          rename(startdate = datetime) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = "alltime",
            stat = "5pct",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_pct95 <- 
          dataset %>%
          select(participantidentifier, datetime, concept, value,) %>%
          group_by(participantidentifier, concept) %>%
          mutate("quantile" = quantile(as.numeric(value), 0.95, na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = quantile) %>%
          rename(startdate = datetime) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = "alltime",
            stat = "95pct",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_mean <- 
          dataset %>%
          select(participantidentifier, datetime, concept, value,) %>%
          group_by(participantidentifier, concept) %>%
          mutate("mean" = mean(as.numeric(value), na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = mean) %>%
          rename(startdate = datetime) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = "alltime",
            stat = "mean",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_median <- 
          dataset %>%
          select(participantidentifier, datetime, concept, value,) %>%
          group_by(participantidentifier, concept) %>%
          mutate("median" = median(as.numeric(value), na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = median) %>%
          rename(startdate = datetime) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = "alltime",
            stat = "median",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_variance <- 
          dataset %>%
          select(participantidentifier, datetime, concept, value,) %>%
          group_by(participantidentifier, concept) %>%
          mutate("variance" = var(as.numeric(value), na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = variance) %>%
          rename(startdate = datetime) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = "alltime",
            stat = "variance",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_numrecords <- 
          dataset %>%
          select(participantidentifier, datetime, concept, value,) %>%
          group_by(participantidentifier, concept) %>%
          drop_na() %>%
          add_count() %>%
          select(-c(value)) %>%
          rename(value = n) %>%
          rename(startdate = datetime) %>% 
          mutate(
            startdate = as_date(min(startdate)),
            enddate = NA,
            timescale = "alltime",
            stat = "numrecords",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        weekly_pct5 <- 
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
          summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T), .groups = "keep") %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              startdate - wday(startdate) + days(1),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = "weekly",
            stat = "5pct",
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
        
        weekly_pct95 <- 
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
          summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T), .groups = "keep") %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              startdate - wday(startdate) + days(1),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = "weekly",
            stat = "95pct",
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
        
        weekly_mean <- 
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
          summarise("value" = mean(as.numeric(value), na.rm = T), .groups = "keep") %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              startdate - wday(startdate) + days(1),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = "weekly",
            stat = "mean",
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
        
        weekly_median <- 
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
          summarise("value" = median(as.numeric(value), na.rm = T), .groups = "keep") %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              startdate - wday(startdate) + days(1),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = "weekly",
            stat = "median",
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
        
        weekly_variance <- 
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
          summarise("value" = var(as.numeric(value), na.rm = T), .groups = "keep") %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              startdate - wday(startdate) + days(1),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = "weekly",
            stat = "variance",
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
        
        weekly_numrecords <-
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
          drop_na() %>%
          count() %>%
          rename(value = n) %>%
          ungroup() %>%
          mutate(
            week_summary_start_date =
              startdate - wday(startdate) + days(1),
            week_summary_end_date =
              week_summary_start_date + days(6),
            timescale = "weekly",
            stat = "numrecords",
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
      } else {
        stop("Error: date column not found")
      }
    }
  }
  
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

