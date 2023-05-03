## About this Project {#sec-about-this-project}

This package provides functions to help with fetching data from Synapse, as well as processing, summarizing, formatting the data, and storing any output in Synapse. While this package is mainly intended for use in RECOVER, some functions in the package can be used outside this context and for general and other specific use cases.

## Requirements {#sec-requirements}

-   R (local, rocker image, etc.)
-   Synapse account with relevant project access (for help with Synapse please refer to the [Synapse documentation](https://help.synapse.org/docs/))
-   Synapse personal access token (`SYNAPSE_AUTH_TOKEN`)

## Installation {#sec-installation}

Two common methods to install and use this package include via your local environment or Docker. The advantage of using Docker over a local installation and use of this package is that:

1.  The Dockerfile in this repo contains instructions to create an environment with all dependencies needed for the system and R
2.  Once the Docker image is built and you will have access to an RStudio instance that you can connect to in any web browser from your local network, eliminating the need to have R installed on your local machine

### Local {#sec-local}

*Requires local installation of R*

1.  Install the package via GitHub

    Currently, `recoverSummarizeR` is not available via CRAN, so it must be installed from GitHub using the `devtools` package.

    ``` r
    install.packages("devtools")
    require(devtools)
    install_github("Sage-Bionetworks/recoverSummarizeR")
    ```

2.  Attach the package

    ``` R
    library(recoverSummarizeR)
    ```

### Docker {#sec-docker}

1.  Modify your shell profile

    A.  Open the file

        ``` sh
        nano ~/.bash_profile
        ```

    B.  Append the following

        ``` bash
        SYNAPSE_AUTH_TOKEN=<your-synapse-personal-access-token>
        export SYNAPSE_AUTH_TOKEN
        ```

    C.  Save the file

        ``` sh
        source ~/.bash_profile
        ```

2.  Build the docker image (two options):

    A.  From the directory containing the Dockerfile

        ``` sh
        cd /path/to/Dockerfile
        docker build <optional-arguments> -t <image-name> .
        ```

        OR

    B.  From anywhere

        ``` sh
        docker build <optional-arguments> -t <image-name> -f <path-to-Dockerfile> .
        ```

3.  Run the docker container

    A.  The value you assign to `PASSWORD` in `docker run ...` will be the password you use to login to the RStudio instance

        ``` sh
        docker run -d -p 8787:8787 --name <container-name> -e PASSWORD=<your-password> -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN <image-name>
        ```

4.  Forward local port 8787 (preferably in a new console window)

5.  Connect to RStudio Server at <http://localhost:8787/>

6.  Login to the RStudio instance with username/password: `rstudio`/`your-password` (your-password was created when running `docker run ...`

7.  Attach the package

    ``` R
    library(recoverSummarizeR)
    ```

You must pass `SYNAPSE_AUTH_TOKEN` as an environment variable to `docker run ...`, unless the variable already exists in your shell profile, e.g., `~/.bash_profile`, and has been exported in the same session. A Synapse authentication token is required for use of the Synapse APIs (e.g. the `synapser` package for R).

For help with Synapse, Synapse APIs, Synapse personal access tokens, etc., please refer to the [Synapse documentation](https://help.synapse.org/docs/).

## Quickstart {#sec-quickstart}

You can use the package's functions as needed, or, for RECOVER, you can use the `mainflow()` function with just a few arguments to run the entire pipeline intended for summarization and egress of data from MHP to DRC.

Using `mainflow()` allows you to use the built-in pipeline with pre-determined logic, formatting, and output specifications. Use `mainflow()` with caution, as `mainflow()` is a purpose-built function based on a pipeline that is tailored to a specific use case in RECOVER and is not intended for general use.

The flow of the pipeline that `mainflow()` is built on is as follows:

### `mainflow()` Pipeline {#sec-main-pipeline}

1.  Get ontology file (i2b2 concepts map)

    ``` r
    get_concept_map(synID)
    ```

2.  Read data files to data frames

    A.  Get post-ETL data files

        ``` r
        synget_parquet_to_df(synDirID, name_filter)
        ```

    B.  Combine partitioned (multi-part) datasets

        ``` r
        combine_duplicate_dfs(df_list)
        ```

3.  Process and transform the data

    A.  Get the excluded (non-approved) i2b2 summary concepts

        ``` r
        diff_concepts(df_list, concept_replacements, concept_map, concept_filter_col)
        ```

    B.  Reshape the data frames that have the relevant data

        ``` r
        melt_df(df, excluded_concepts)
        ```

    C.  Convert the `value` column of relevant data frames to `numeric` type

        ``` r
        convert_col_to_numeric(df_list, df_to_avoid, col_to_convert)
        ```

4.  Summarize the relevant data

    ``` r
    stat_summarize(df)
    ```

5.  Process and transform the output into the desired format for i2b2

    ``` r
    process_df(df, concept_map, concept_replacements_reversed, concept_map_concepts, concept_map_units)
    ```

6.  Write the output data frames to CSV files

    ``` r
    write.csv(output_concepts, file = 'output_concepts.csv', row.names = F)
    write.csv(concept_map, file = 'concepts_map.csv', row.names = F)
    ```

7.  Store the output in Synapse

    ``` r
    store_in_syn(synFolderID, filepath, used_param, executed_param)
    ```
