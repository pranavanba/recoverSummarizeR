summary <- function(dataset) {
  
  if ("StartDate" %in% colnames(dataset) & "EndDate" %in% colnames(dataset)) {
    all_pct5 <- 
      dataset %>%
      select(ParticipantIdentifier, StartDate, EndDate, concept, value,) %>%
      group_by(ParticipantIdentifier, concept) %>%
      mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
      select(-c(value)) %>%
      rename(value = quantile) %>%
      mutate(
        StartDate = as_date(min(StartDate)),
        EndDate = as_date(max(EndDate)),
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
        StartDate = as_date(min(StartDate)),
        EndDate = as_date(max(EndDate)),
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
        StartDate = as_date(min(StartDate)),
        EndDate = as_date(max(EndDate)),
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
        StartDate = as_date(min(StartDate)),
        EndDate = as_date(max(EndDate)),
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
        StartDate = as_date(min(StartDate)),
        EndDate = as_date(max(EndDate)),
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
        StartDate = as_date(min(StartDate)),
        EndDate = as_date(max(EndDate)),
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
        StartDate = as_date(StartDate),
        year = year(StartDate),
        week = epiweek(StartDate)
      ) %>%
      filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
      group_by(ParticipantIdentifier, concept, year, week) %>%
      summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T), .groups = "keep") %>%
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
        StartDate = as_date(StartDate),
        year = year(StartDate),
        week = epiweek(StartDate)
      ) %>%
      filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
      group_by(ParticipantIdentifier, concept, year, week) %>%
      summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T), .groups = "keep") %>%
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
        StartDate = as_date(StartDate),
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
        StartDate = as_date(StartDate),
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
        StartDate = as_date(StartDate),
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
        StartDate = as_date(StartDate),
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
  } else {
    if ("Date" %in% colnames(dataset)) {
      all_pct5 <- 
        dataset %>%
        select(ParticipantIdentifier, Date, concept, value,) %>%
        group_by(ParticipantIdentifier, concept) %>%
        mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = quantile) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(min(StartDate)),
          EndDate = NA,
          timescale = "alltime",
          stat = "5pct",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_pct95 <- 
        dataset %>%
        select(ParticipantIdentifier, Date, concept, value,) %>%
        group_by(ParticipantIdentifier, concept) %>%
        mutate("quantile" = quantile(as.numeric(value), 0.95, na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = quantile) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(min(StartDate)),
          EndDate = NA,
          timescale = "alltime",
          stat = "95pct",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_mean <- 
        dataset %>%
        select(ParticipantIdentifier, Date, concept, value,) %>%
        group_by(ParticipantIdentifier, concept) %>%
        mutate("mean" = mean(as.numeric(value), na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = mean) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(min(StartDate)),
          EndDate = NA,
          timescale = "alltime",
          stat = "mean",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_median <- 
        dataset %>%
        select(ParticipantIdentifier, Date, concept, value,) %>%
        group_by(ParticipantIdentifier, concept) %>%
        mutate("median" = median(as.numeric(value), na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = median) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(min(StartDate)),
          EndDate = NA,
          timescale = "alltime",
          stat = "median",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_variance <- 
        dataset %>%
        select(ParticipantIdentifier, Date, concept, value,) %>%
        group_by(ParticipantIdentifier, concept) %>%
        mutate("variance" = var(as.numeric(value), na.rm = T)) %>%
        select(-c(value)) %>%
        rename(value = variance) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(min(StartDate)),
          EndDate = NA,
          timescale = "alltime",
          stat = "variance",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      all_numrecords <- 
        dataset %>%
        select(ParticipantIdentifier, Date, concept, value,) %>%
        group_by(ParticipantIdentifier, concept) %>%
        drop_na() %>%
        add_count() %>%
        select(-c(value)) %>%
        rename(value = n) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(min(StartDate)),
          EndDate = NA,
          timescale = "alltime",
          stat = "numrecords",
          concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
        ) %>%
        distinct() %>%
        ungroup() %>%
        select(-c(timescale, stat))
      
      weekly_pct5 <- 
        dataset %>%
        select(ParticipantIdentifier, concept, value, Date) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(StartDate),
          year = year(StartDate),
          week = epiweek(StartDate)
        ) %>%
        filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
        group_by(ParticipantIdentifier, concept, year, week) %>%
        summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T), .groups = "keep") %>%
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
        select(ParticipantIdentifier, concept, value, Date) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(StartDate),
          year = year(StartDate),
          week = epiweek(StartDate)
        ) %>%
        filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
        group_by(ParticipantIdentifier, concept, year, week) %>%
        summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T), .groups = "keep") %>%
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
        select(ParticipantIdentifier, concept, value, Date) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(StartDate),
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
        select(ParticipantIdentifier, concept, value, Date) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(StartDate),
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
        select(ParticipantIdentifier, concept, value, Date) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(StartDate),
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
        select(ParticipantIdentifier, concept, value, Date) %>%
        rename(StartDate = Date) %>% 
        mutate(
          StartDate = as_date(StartDate),
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
    } else {
      if ("DateTime" %in% colnames(dataset)) {
        all_pct5 <- 
          dataset %>%
          select(ParticipantIdentifier, DateTime, concept, value,) %>%
          group_by(ParticipantIdentifier, concept) %>%
          mutate("quantile" = quantile(as.numeric(value), 0.05, na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = quantile) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(min(StartDate)),
            EndDate = NA,
            timescale = "alltime",
            stat = "5pct",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_pct95 <- 
          dataset %>%
          select(ParticipantIdentifier, DateTime, concept, value,) %>%
          group_by(ParticipantIdentifier, concept) %>%
          mutate("quantile" = quantile(as.numeric(value), 0.95, na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = quantile) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(min(StartDate)),
            EndDate = NA,
            timescale = "alltime",
            stat = "95pct",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_mean <- 
          dataset %>%
          select(ParticipantIdentifier, DateTime, concept, value,) %>%
          group_by(ParticipantIdentifier, concept) %>%
          mutate("mean" = mean(as.numeric(value), na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = mean) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(min(StartDate)),
            EndDate = NA,
            timescale = "alltime",
            stat = "mean",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_median <- 
          dataset %>%
          select(ParticipantIdentifier, DateTime, concept, value,) %>%
          group_by(ParticipantIdentifier, concept) %>%
          mutate("median" = median(as.numeric(value), na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = median) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(min(StartDate)),
            EndDate = NA,
            timescale = "alltime",
            stat = "median",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_variance <- 
          dataset %>%
          select(ParticipantIdentifier, DateTime, concept, value,) %>%
          group_by(ParticipantIdentifier, concept) %>%
          mutate("variance" = var(as.numeric(value), na.rm = T)) %>%
          select(-c(value)) %>%
          rename(value = variance) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(min(StartDate)),
            EndDate = NA,
            timescale = "alltime",
            stat = "variance",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        all_numrecords <- 
          dataset %>%
          select(ParticipantIdentifier, DateTime, concept, value,) %>%
          group_by(ParticipantIdentifier, concept) %>%
          drop_na() %>%
          add_count() %>%
          select(-c(value)) %>%
          rename(value = n) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(min(StartDate)),
            EndDate = NA,
            timescale = "alltime",
            stat = "numrecords",
            concept = paste0("mhp:summary:", timescale, ":", stat, ":", concept)
          ) %>%
          distinct() %>%
          ungroup() %>%
          select(-c(timescale, stat))
        
        weekly_pct5 <- 
          dataset %>%
          select(ParticipantIdentifier, concept, value, DateTime) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(StartDate),
            year = year(StartDate),
            week = epiweek(StartDate)
          ) %>%
          filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
          group_by(ParticipantIdentifier, concept, year, week) %>%
          summarise("value" = quantile(as.numeric(value), 0.05, na.rm = T), .groups = "keep") %>%
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
          select(ParticipantIdentifier, concept, value, DateTime) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(StartDate),
            year = year(StartDate),
            week = epiweek(StartDate)
          ) %>%
          filter(StartDate >= floor_date(min(StartDate), unit = "week", week_start = "Sunday")) %>%
          group_by(ParticipantIdentifier, concept, year, week) %>%
          summarise("value" = quantile(as.numeric(value), 0.95, na.rm = T), .groups = "keep") %>%
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
          select(ParticipantIdentifier, concept, value, DateTime) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(StartDate),
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
          select(ParticipantIdentifier, concept, value, DateTime) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(StartDate),
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
          select(ParticipantIdentifier, concept, value, DateTime) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(StartDate),
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
          select(ParticipantIdentifier, concept, value, DateTime) %>%
          rename(StartDate = DateTime) %>% 
          mutate(
            StartDate = as_date(StartDate),
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
      } else {
        stop("Error: Date column not found")
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

