## recoverSummarizeR

[![GitHub R package version](https://img.shields.io/github/r-package/v/pranavanba/recoversummarizer?label=R%20Package%20Version)](DESCRIPTION)
[![GitHub](https://img.shields.io/github/license/pranavanba/recoversummarizer)](LICENSE.md)
[![GitHub Workflow Status (with branch)](https://img.shields.io/github/actions/workflow/status/pranavanba/recoversummarizer/docker-build-publish.yaml?branch=main&label=docker-build-publish&logo=github%20actions&logoColor=white)](https://github.com/pranavanba/recoverSummarizeR/actions/workflows/docker-build-publish.yaml)

This package provides functions to help with fetching data from Synapse, as well as processing, summarizing, formatting the data, and storing any output in Synapse. While this package is mainly intended for use in RECOVER, some functions in the package can be used outside this context and for general and other specific use cases.

## Requirements

-   R >= v4.0.0
-   Synapse account with relevant access permissions
-   Synapse authentication token

A Synapse authentication token is required for use of the Synapse APIs (e.g. the `synapser` package for R). For help with Synapse, Synapse APIs, Synapse authentication tokens, etc., please refer to the [Synapse documentation](https://help.synapse.org/docs/).

## Installation and Usage

There are two methods to install and use this package: as a **standalone package** or **Docker container**. Please refer to the [Standalone Package](#standalone-package) and [Docker Container](#docker-container) sections for their respective installation and usage instructions.

### Standalone Package

Currently, `recoverSummarizeR` is not available via CRAN, so it must be installed from GitHub using the `devtools` package.

```R
install.packages("devtools")
devtools::install_github("Sage-Bionetworks/recoverSummarizeR")
```

### Docker Container

For the Docker method, there is a pre-published docker image available at [Packages](https://github.com/orgs/Sage-Bionetworks/packages/container/package/recoversummarizer).

The primary purpose of using the Docker method, in the context of this package, is that the docker image published from this repo contains instructions to:

1. Create an environment with all of the dependencies needed for the system and R
2. Install the `recoverSummarizeR` package
3. Automatically execute the `summarize_pipeline()` function with input arguments for `summarize_pipeline()` provided as environment variables during the run process in the `docker run ...` command using the `-e` flag

#### Use the pre-built Docker image

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

2.  Pull the docker image

```Shell
docker pull ghcr.io/sage-bionetworks/recoversummarizer:main
```

3.  Run the docker container

```Shell
docker run \
  --name <container-name> \
  -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN \
  -e ONTOLOGY_FILE_ID=<synapseID> \
  -e PARQUET_DIR_ID=<synapseID> \
  -e DATASET_NAME_FILTER=<string> \
  -e CONCEPT_REPLACEMENTS=<named-vector-in-parentheses> \
  -e CONCEPT_FILTER_COL=<concept-map-column-name> \
  -e SYN_FOLDER_ID=<synapseID> \
  ghcr.io/sage-bionetworks/recoversummarizer:main
```

For an explanation of the various environment variables required in the `docker run` command, please see [Environment Variables](#environment-variables).

#### Build the Docker image yourself

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

2. Clone this repo

3.  Build the docker image

```Shell
# Option 1: From the directory containing the Dockerfile
cd /path/to/Dockerfile
docker build <optional-arguments> -t <image-name> .

# Option 2: From anywhere
docker build <optional-arguments> -t <image-name> -f <path-to-Dockerfile> .
```

4.  Run the docker container

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

For an explanation of the various environment variables required in the `docker run` command, please see [Environment Variables](#environment-variables).

#### Environment Variables

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

### `summarize_pipeline()`

```R
# Get ontology file (i2b2 concepts map)
get_concept_map(synID)

# Read data files to data frames

# Get post-ETL data files
synget_parquet_to_df(synDirID, dataset_name_filter)

# Combine partitioned (multi-part) datasets
combine_duplicate_dfs(df_list)

# Process and transform the data

# Get the excluded (non-approved) i2b2 summary concepts
diff_concepts(df_list, concept_replacements, concept_map, concept_filter_col)

# Reshape the data frames that have the relevant data
melt_df(df, excluded_concepts)

# Convert the `value` column of relevant data frames to `numeric` type
convert_col_to_numeric(df_list, df_to_avoid, col_to_convert)

# Summarize the relevant data
stat_summarize(df)

# Process and transform the output into the desired format for i2b2
process_df(df, concept_map, concept_replacements_reversed, concept_map_concepts, concept_map_units)

# Write the output data frames to CSV files
write.csv(output_concepts, file = 'output_concepts.csv', row.names = F)
write.csv(concept_map, file = 'concepts_map.csv', row.names = F)

# Store the output in Synapse
store_in_syn(synFolderID, filepath, used_param, executed_param)
```
