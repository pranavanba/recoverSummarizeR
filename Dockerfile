FROM rocker/r-ver:4.2.2

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y git python3 python3-pip python3-venv curl libssl-dev libcurl4-openssl-dev

RUN Rscript -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'
RUN Rscript -e 'install.packages("devtools")'
RUN Rscript -e 'require(devtools)'
RUN Rscript -e 'devtools::install_github("Sage-Bionetworks/recoverSummarizeR", ref = "release")'

CMD ["sh", "-c", "Rscript -e 'library(recoverSummarizeR); \
                      synapser::synLogin(); \
                      ontologyFileID <- Sys.getenv(\"ontologyFileID\"); \
                      parquetDirID <- Sys.getenv(\"PARQUET_DIR_ID\"); \
                      name_filter <- Sys.getenv(\"DATASET_NAME_FILTER\"); \
                      concept_replacements <- eval(parse(text=Sys.getenv(\"CONCEPT_REPLACEMENTS\"))); \
                      concept_filter_col <- Sys.getenv(\"CONCEPT_FILTER_COL\"); \
                      synFolderID <- Sys.getenv(\"SYN_FOLDER_ID\"); \
                      recoverSummarizeR::mainflow(ontologyFileID, parquetDirID, \
                      concept_replacements, concept_filter_col, synFolderID)'"]
