## About this Project

This package provides functions to help with fetching data from Synapse, as well as processing, summarizing, formatting the data, and storing any output in Synapse. While this package is mainly intended for use in RECOVER, some functions in the package can be used outside this context and for general and other specific use cases.

## Requirements

-   R (local, rocker image, etc.)
-   Synapse account with relevant project access (for help with Synapse please refer to the [Synapse documentation](https://help.synapse.org/docs/))
-   Synapse personal access token (`SYNAPSE_AUTH_TOKEN`)

## Installation and Usage

Two methods to install and use this package include via your local environment or Docker. The primary purpose of using the Docker method instead of the local installation, in the context of this package, is that the Dockerfile in this repo contains instructions to create an environment with all dependencies needed for the system and R, install the `recoverSummarizeR` package, and automatically execute the `mainflow()` function with input arguments for `mainflow()` provided during the build process in the `docker run ...` command.

### Local

*Requires local installation of R*

1.  Install the package via GitHub

Currently, `recoverSummarizeR` is not available via CRAN, so it must be installed from GitHub using the `devtools` package.

```R
install.packages("devtools")
require(devtools)
install_github("Sage-Bionetworks/recoverSummarizeR")
```

2.  Attach the package

```R
library(recoverSummarizeR)
```

### Docker

1.  Add your Synapse personal access token to the environment (2 options)

    A. For only the current shell session:

    ```Shell
    export SYNAPSE_AUTH_TOKEN=<your-token>
    ```

    B. For all future shell sessions (modify your shell profile)

    1.  Open the shell profile file (using bash profile as an example)

        ```Shell
        nano ~/.bash_profile
        ```

    2.  Append the following

        ```Shell
        SYNAPSE_AUTH_TOKEN=<your-synapse-personal-access-token>
        export SYNAPSE_AUTH_TOKEN
        ```

    3.  Save the file

        ```Shell
        source ~/.bash_profile
        ```

2.  Build the docker image (two options):

    A.  From the directory containing the Dockerfile

    ```Shell
    cd /path/to/Dockerfile
    docker build <optional-arguments> -t <image-name> .
    ```

    OR

    B.  From anywhere

    ```Shell
    docker build <optional-arguments> -t <image-name> -f <path-to-Dockerfile> .
    ```

3.  Run the docker container

```Shell
docker run --name <docker-container-name> -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN -e ONTOLOGY_FILE_ID=<synapseID> -e PARQUET_DIR_ID=<synapseID> -e DATASET_NAME_FILTER=<string> -e CONCEPT_REPLACEMENTS=<named-vector-in-parentheses> -e CONCEPT_FILTER_COL=<concept-map-column-name> -e SYN_FOLDER_ID=<synapseID> <docker-image-name>
```

A Synapse authentication token is required for use of the Synapse APIs (e.g. the `synapser` package for R). For help with Synapse, Synapse APIs, Synapse personal access tokens, etc., please refer to the [Synapse documentation](https://help.synapse.org/docs/).

The environment variables passed to `docker run ...` are the input arguments of `mainflow()`, and as such must be provided in order to use the docker method.

+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Variable               | Definition                                                                                                                                                                                                       | Example                                                                                                                                                                  |
+========================+==================================================================================================================================================================================================================+==========================================================================================================================================================================+
| `ONTOLOGY_FILE_ID`     | A Synapse ID for a CSV file stored in Synapse. For RECOVER, this file is the i2b2 concepts map.                                                                                                                  | syn12345678                                                                                                                                                              |
+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `PARQUET_DIR_ID`       | A Synapse ID for a folder entity in Synapse where the data is stored. For RECOVER, this would be the folder housing the post-ETL parquet data.                                                                   | syn12345678                                                                                                                                                              |
+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `DATASET_NAME_FILTER`  | A string found in the names of the files to be read. This acts like a filter to include only the files that contain the string in their names.                                                                   | fitbit                                                                                                                                                                   |
+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `CONCEPT_REPLACEMENTS` | A named vector of strings and their replacements. The names must be valid values of the `concept_filter_col` column of the `concept_map` data frame. For RECOVER, `concept_map` is the ontology file data frame. | "c('mins' = 'minutes', 'avghr' = 'averageheartrate', 'spo2' = 'spo2\_', 'hrv' = 'hrv_dailyrmssd', 'restinghr' = 'restingheartrate', 'sleepbrth' = 'sleepsummarybreath')" |
|                        |                                                                                                                                                                                                                  |                                                                                                                                                                          |
|                        |                                                                                                                                                                                                                  | *Must surround `c(…)` in parentheses in `docker run …`*                                                                                                                  |
+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `CONCEPT_FILTER_COL`   | The column of the `concept_map` data frame that contains "approved concepts" (column names of dataset data frames that are not to be excluded). For RECOVER, `concept_map` is the ontology file data frame.      | concept_cd                                                                                                                                                               |
+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `SYN_FOLDER_ID`        | A Synapse ID for a folder entity in Synapse where you want to store a file.                                                                                                                                      | syn12345678                                                                                                                                                              |
+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

## Quick Start

You can use the package's functions as needed, or, for RECOVER, you can use the `mainflow()` function with just a few arguments to run the entire pipeline intended for summarization and egress of data from MHP to DRC.

Using `mainflow()` allows you to use the built-in pipeline with pre-determined logic, formatting, and output specifications. Use `mainflow()` with caution, as `mainflow()` is a purpose-built function based on a pipeline that is tailored to a specific use case in RECOVER and is not intended for general use.

The flow of the pipeline that `mainflow()` is built on is as follows:

### `mainflow()` Pipeline

```R
# 1.  Get ontology file (i2b2 concepts map)

get_concept_map(synID)

# 2.  Read data files to data frames

    # A.  Get post-ETL data files

    synget_parquet_to_df(synDirID, name_filter)

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
