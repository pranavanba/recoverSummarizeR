FROM rocker/r-ver:4.2.2

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y git python3 python3-pip python3-venv curl libssl-dev libcurl4-openssl-dev

CMD Rscript -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org")); \
              require(synapser); \
              install.packages("devtools"); \
              require(devtools); \
              devtools::install_github("Sage-Bionetworks/recoverSummarizeR"); \
              library(recoverSummarizeR); \
              synapser::synLogin(); \
              ontologyFileID <- Sys.getenv("ontologyFileID"); \
              parquetDirID <- Sys.getenv("PARQUET_DIR_ID"); \
              dataset_name_filter <- Sys.getenv("DATASET_NAME_FILTER"); \
              concept_replacements <- eval(parse(text=Sys.getenv("CONCEPT_REPLACEMENTS"))); \
              concept_filter_col <- Sys.getenv("CONCEPT_FILTER_COL"); \
              synFolderID <- Sys.getenv("SYN_FOLDER_ID"); \
              recoverSummarizeR::summarize_pipeline(ontologyFileID, parquetDirID, dataset_name_filter, concept_replacements, concept_filter_col, synFolderID)'
