## recoverSummarizeR

This package provides functions to help with fetching data from Synapse, as well as processing, summarizing, formatting the data, and storing any output in Synapse. While this package is mainly intended for use in RECOVER, some functions in the package can be used outside this context and for general and other specific use cases.

## Requirements

-   R >= 4.0.0 (local, rocker image, etc.)
-   Synapse account with relevant access permissions (for help with Synapse please refer to the [Synapse documentation](https://help.synapse.org/docs/))
-   Synapse personal access token

## Installation

Currently, `recoverSummarizeR` is not available via CRAN, so it must be installed from GitHub using the `devtools` package.

```R
install.packages("devtools")
devtools::install_github("Sage-Bionetworks/recoverSummarizeR", ref = "release")
```

## Docker

There are two methods to install and use this package: yourself or Docker. The primary purpose of using the Docker method, in the context of this package, is that the Dockerfile in this repo contains instructions to create an environment with all dependencies needed for the system and R, install the `recoverSummarizeR` package, and automatically execute the `summarize_pipeline()` function with input arguments for `summarize_pipeline()` provided during the build process in the `docker run ...` command.

1.  Add your Synapse personal access token to the environment

```Shell
# Option 1: For only the current shell session:
export SYNAPSE_AUTH_TOKEN=<your-token>

# Option 2: For all future shell sessions (modify your shell profile)
# Open the profile file
nano ~/.bash_profile

# Append the following
SYNAPSE_AUTH_TOKEN=<your-token>
export SYNAPSE_AUTH_TOKEN

# Save the file
source ~/.bash_profile
```

2.  Build the docker image

```Shell
# Option 1: From the directory containing the Dockerfile
cd /path/to/Dockerfile
docker build <optional-arguments> -t <image-name> .

# Option 2: From anywhere
docker build <optional-arguments> -t <image-name> -f <path-to-Dockerfile> .
```

3.  Run the docker container

```Shell
docker run \
  --name <docker-container-name> \
  -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN \
  -e ONTOLOGY_FILE_ID=<synapseID> \
  -e PARQUET_DIR_ID=<synapseID> \
  -e DATASET_NAME_FILTER=<string> \
  -e CONCEPT_REPLACEMENTS=<named-vector-in-parentheses> \
  -e CONCEPT_FILTER_COL=<concept-map-column-name> \
  -e SYN_FOLDER_ID=<synapseID> \
  <docker-image-name>
```

A Synapse authentication token is required for use of the Synapse APIs (e.g. the `synapser` package for R). For help with Synapse, Synapse APIs, Synapse personal access tokens, etc., please refer to the [Synapse documentation](https://help.synapse.org/docs/).

The environment variables passed to `docker run ...` are the input arguments of `summarize_pipeline()`, and as such must be provided in order to use the docker method.

Variable | Definition | Example
---|---|---
| `ONTOLOGY_FILE_ID` | A Synapse ID for a CSV file stored in Synapse. For RECOVER, this file is the i2b2 concepts map. | syn12345678
| `PARQUET_DIR_ID` | A Synapse ID for a folder entity in Synapse where the data is stored. For RECOVER, this would be the folder housing the post-ETL parquet data. | syn12345678
| `DATASET_NAME_FILTER`  | A string found in the names of the files to be read. This acts like a filter to include only the files that contain the string in their names. | fitbit
| `CONCEPT_REPLACEMENTS` | A named vector of strings and their replacements. The names must be valid values of the `concept_filter_col` column of the `concept_map` data frame. For RECOVER, `concept_map` is the ontology file data frame. | "c('mins' = 'minutes', 'avghr' = 'averageheartrate', 'spo2' = 'spo2\_', 'hrv' = 'hrv_dailyrmssd', 'restinghr' = 'restingheartrate', 'sleepbrth' = 'sleepsummarybreath')" <br><br> *Must surround `c(…)` in parentheses (as indicated above) in `docker run …`*
| `CONCEPT_FILTER_COL` | The column of the `concept_map` data frame that contains "approved concepts" (column names of dataset data frames that are not to be excluded). For RECOVER, `concept_map` is the ontology file data frame. | concept_cd
| `SYN_FOLDER_ID` | A Synapse ID for a folder entity in Synapse where you want to store a file. | syn12345678

## Quick Start

You can use the package's functions as needed, or, for RECOVER, you can use the `summarize_pipeline()` function with just a few arguments to run the entire pipeline intended for summarization and egress of data from MHP to DRC.

Using `summarize_pipeline()` allows you to use the built-in pipeline with pre-determined logic, formatting, and output specifications. Use `summarize_pipeline()` with caution, as `summarize_pipeline()` is a purpose-built function based on a pipeline that is tailored to a specific use case in RECOVER and is not intended for general use.

The flow of the pipeline that `summarize_pipeline()` is built on is as follows:

### `summarize_pipeline()` Pipeline

```R
# 1.  Get ontology file (i2b2 concepts map)
get_concept_map(synID)

# 2.  Read data files to data frames

    # A.  Get post-ETL data files
    synget_parquet_to_df(synDirID, dataset_name_filter)

    # B.  Combine partitioned (multi-part) datasets
    combine_duplicate_dfs(df_list)

# 3.  Process and transform the data

    # A.  Get the excluded (non-approved) i2b2 summary concepts
    diff_concepts(df_list, concept_replacements, concept_map, concept_filter_col)

    # B.  Reshape the data frames that have the relevant data
    melt_df(df, excluded_concepts)

    # C.  Convert the `value` column of relevant data frames to `numeric` type
    convert_col_to_numeric(df_list, df_to_avoid, col_to_convert)

# 4.  Summarize the relevant data
stat_summarize(df)

# 5.  Process and transform the output into the desired format for i2b2
process_df(df, concept_map, concept_replacements_reversed, concept_map_concepts, concept_map_units)

# 6.  Write the output data frames to CSV files
write.csv(output_concepts, file = 'output_concepts.csv', row.names = F)
write.csv(concept_map, file = 'concepts_map.csv', row.names = F)

# 7.  Store the output in Synapse
store_in_syn(synFolderID, filepath, used_param, executed_param)
```
