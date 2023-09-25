## recoverSummarizeR

[![GitHub R package version](https://img.shields.io/github/r-package/v/sage-bionetworks/recoversummarizer?label=R%20Package%20Version)](DESCRIPTION)
[![GitHub](https://img.shields.io/github/license/sage-bionetworks/recoversummarizer)](LICENSE.md)

This package provides functions to help with fetching data from Synapse, as well as processing, summarizing, formatting the data, and storing any output in Synapse. While this package is mainly intended for use in RECOVER, some functions in the package can be used outside this context and for general and other specific use cases.

## Requirements

-   R >= v4.0.0
-   Synapse account with relevant access permissions
-   Synapse authentication token

A Synapse authentication token is required for use of the Synapse APIs (e.g. the `synapser` package for R). For help with Synapse, Synapse APIs, Synapse authentication tokens, etc., please refer to the [Synapse documentation](https://help.synapse.org/docs/).

## Installation

Currently, `recoverSummarizeR` is not available via CRAN, so it must be installed from GitHub using the `devtools` package.

```R
install.packages("devtools")
devtools::install_github("Sage-Bionetworks/recoverSummarizeR")
```

## Quick Start

You can use the package's functions as needed, or, for RECOVER, you can use the `summarize_pipeline()` function with just a few arguments to run the entire pipeline intended for summarization and egress of data from MHP to DRC.

Please refer to [summarize_pipeline() Parameters](#summarize_pipeline-parameters) below for an explanation of the input arguments required by the `summarize_pipeline()` function.

Using `summarize_pipeline()` allows you to use the built-in pipeline with pre-determined logic, formatting, and output specifications. Use `summarize_pipeline()` with caution, as `summarize_pipeline()` is a purpose-built function based on a pipeline tailored to a specific use case in RECOVER and is not intended for general use.

The flow of the pipeline that `summarize_pipeline()` is based on is as follows:

1. Get ontology file (i2b2 concepts map)
2. Read data files to data frames
3. Combine partitioned (multi-part) datasets
4. Process and transform the data
5. Get the excluded (non-approved) i2b2 summary concepts
6. Reshape the data frames that have the relevant data
7. Convert the `value` column of relevant data frames to `numeric` type
8. Summarize the relevant data
9. Process and transform the output into the desired format for i2b2
10. Write the output data frames to CSV files
11. Store the output in Synapse

#### `summarize_pipeline()` Parameters

The parameters passed to `summarize_pipeline()` must be provided in order to use this all-in-one function.

Variable | Definition | Example
---|---|---
| `ONTOLOGY_FILE_ID` | A Synapse ID for a CSV file stored in Synapse. For RECOVER, this file is the i2b2 concepts map. | syn12345678
| `PARQUET_DIR_ID` | A Synapse ID for a folder entity in Synapse where the data is stored. For RECOVER, this would be the folder housing the post-ETL parquet data. | syn12345678
| `DATASET_NAME_FILTER`  | A string found in the names of the files to be read. This acts like a filter to include only the files that contain the string in their names. | fitbit
| `CONCEPT_REPLACEMENTS` | A named vector of strings and their replacements. The names must be valid values of the `concept_filter_col` column of the `concept_map` data frame. For RECOVER, `concept_map` is the ontology file data frame. | "c('mins' = 'minutes', 'avghr' = 'averageheartrate', 'spo2' = 'spo2\_', 'hrv' = 'hrv_dailyrmssd', 'restinghr' = 'restingheartrate', 'sleepbrth' = 'sleepsummarybreath')" <br><br> *Must surround `c(…)` in parentheses (as indicated above) in `docker run …`*
| `CONCEPT_FILTER_COL` | The column of the `concept_map` data frame that contains "approved concepts" (column names of dataset data frames that are not to be excluded). For RECOVER, `concept_map` is the ontology file data frame. | concept_cd
| `SYN_FOLDER_ID` | A Synapse ID for a folder entity in Synapse where you want to store a file. | syn12345678
| `method` | Either `synapse` or `sts` to specify the method to use in getting the parquet datasets. `synapse` will get files directly from a synapse project or folder using the synapse client, while `sts` will use sts-token access to get objects from an sts-enabled storage location, such as an S3 bucket. | synapse
| `s3bucket` | The name of the S3 bucket to access when `method=sts`. | my-bucket
| `s3basekey` | The base key of the S3 bucket to access when `method='sts'`. | main/parquet/
| `downloadLocation` | The location to download input files to. | ./parquet
