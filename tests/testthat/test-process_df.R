test_that("function works correctly", {
  df <- data.frame(participantidentifier = c(1, 2, 3, 4),
                   startdate = c("2022-01-01", "2022-01-01", "2022-01-02", "2022-01-03"),
                   enddate = c("2022-01-03", "2022-01-02", "2022-01-03", "2022-01-04"),
                   concept = c("summary:BLOOD_PRESSURE_SYSTOLIC", "summary:BLOOD_PRESSURE_DIASTOLIC",
                               "summary:HEART_RATE", "summary:RESPIRATORY_RATE"),
                   value = c(120, 80, 70, 20))
  concept_map <- data.frame(concept_cd = c("summary:BP_SYSTOLIC", "summary:BP_DIASTOLIC",
                                           "summary:HR", "summary:RR"),
                            valtype_cd = c("N", "N", "N", "N"),
                            UNITS_CD = c("mmHg", "mmHg", "beats/min", "breaths/min"))
  concept_replacements <- c("bp" = "blood_pressure", "hr" = "heart_rate", "rr" = "respiratory_rate")
  concept_replacements_reversed <- reverse_str_pairs(concept_replacements)
  df_processed <- process_df(df, concept_map, concept_replacements_reversed, "concept_cd", "UNITS_CD")
  
  expect_no_error(process_df(df, concept_map, concept_replacements_reversed, "concept_cd", "UNITS_CD"))
})

test_that("incorrect input types raise an error", {
  df <- data.frame(participantidentifier = c(1, 2, 3, 4),
                   startdate = c("2022-01-01", "2022-01-01", "2022-01-02", "2022-01-03"),
                   enddate = c("2022-01-03", "2022-01-02", "2022-01-03", "2022-01-04"),
                   concept = c("summary:BLOOD_PRESSURE_SYSTOLIC", "summary:BLOOD_PRESSURE_DIASTOLIC",
                               "summary:HEART_RATE", "summary:RESPIRATORY_RATE"),
                   value = c(120, 80, 70, 20))
  concept_map <- data.frame(concept_cd = c("summary:BP_SYSTOLIC", "summary:BP_DIASTOLIC",
                                           "summary:HR", "summary:RR"),
                            valtype_cd = c("N", "N", "N", "N"),
                            UNITS_CD = c("mmHg", "mmHg", "beats/min", "breaths/min"))
  concept_replacements <- c("bp" = "blood_pressure", "hr" = "heart_rate", "rr" = "respiratory_rate")
  concept_replacements_reversed <- reverse_str_pairs(concept_replacements)
  
  expect_error(process_df(df = "a", 
                          concept_map = concept_map, 
                          concept_replacements_reversed = concept_replacements_reversed, 
                          concept_map_concepts = "concept_cd", 
                          concept_map_units = "UNITS_CD"))
  expect_error(process_df(df = df, 
                          concept_map = 1, 
                          concept_replacements_reversed = concept_replacements_reversed, 
                          concept_map_concepts = "concept_cd", 
                          concept_map_units = "UNITS_CD"))
  expect_error(process_df(df = df, 
                          concept_map = concept_map, 
                          concept_replacements_reversed = list("bp", "1", NA), 
                          concept_map_concepts = "concept_cd", 
                          concept_map_units = "UNITS_CD"))
  expect_error(process_df(df = df, 
                          concept_map = concept_map, 
                          concept_replacements_reversed = concept_replacements_reversed, 
                          concept_map_concepts = "concept_col", 
                          concept_map_units = "UNITS_CD"))
  expect_error(process_df(df = df, 
                          concept_map = concept_map, 
                          concept_replacements_reversed = concept_replacements_reversed, 
                          concept_map_concepts = "concept_cd", 
                          concept_map_units = "UNITS_COL"))
})

test_that("not providing concept_replacements_reversed raises no error", {
  df <- data.frame(participantidentifier = c(1, 2, 3, 4),
                   startdate = c("2022-01-01", "2022-01-01", "2022-01-02", "2022-01-03"),
                   enddate = c("2022-01-03", "2022-01-02", "2022-01-03", "2022-01-04"),
                   concept = c("summary:BLOOD_PRESSURE_SYSTOLIC", "summary:BLOOD_PRESSURE_DIASTOLIC",
                               "summary:HEART_RATE", "summary:RESPIRATORY_RATE"),
                   value = c(120, 80, 70, 20))
  concept_map <- data.frame(concept_cd = c("summary:BLOOD_PRESSURE_SYSTOLIC", "summary:BLOOD_PRESSURE_DIASTOLIC",
                                           "summary:HEART_RATE", "summary:RESPIRATORY_RATE"),
                            valtype_cd = c("N", "N", "N", "N"),
                            UNITS_CD = c("mmHg", "mmHg", "beats/min", "breaths/min"))
  
  expect_no_error(process_df(df = df, concept_map = concept_map,
                             concept_map_concepts = "concept_cd", concept_map_units = "UNITS_CD"))
})
