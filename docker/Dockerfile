FROM rocker/rstudio:latest

ARG GITHUB_TOKEN
ARG username

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y git python3 python3-pip python3-venv curl

USER rstudio

RUN echo "GITHUB_TOKEN: ${GITHUB_TOKEN}"
RUN echo "username: ${username}"

RUN cd /home/rstudio && \
    git clone https://${username}:${GITHUB_TOKEN}@github.com/pranavanba/convert2i2b2.git && \
    chown -R rstudio:rstudio /home/rstudio/convert2i2b2

RUN Rscript -e 'install.packages(c("reticulate", "magrittr", "dplyr", "tidyr", "tibble", "jsonlite", "stringr", "arrow", "readr", "reshape2", "lubridate", "purrr"))'
RUN Rscript -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'

USER root

EXPOSE 8787

CMD ["/init"]
