FROM rocker/rstudio:latest

ARG GITHUB_TOKEN
ARG username

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y git python3 python3-pip python3-venv curl

USER rstudio

<<<<<<< HEAD
COPY /home/ec2-user/convert2i2b2 /home/rstudio/convert2i2b2

RUN chown -R rstudio:rstudio /home/rstudio/convert2i2b2
=======
RUN cd /home/rstudio && \
    git clone https://${username}:${GITHUB_TOKEN}@github.com/pranavanba/convert2i2b2.git && \
    chown -R rstudio:rstudio /home/rstudio/convert2i2b2
>>>>>>> parent of 2cf2097 (Copy repo from home dir instead of git clone)

RUN Rscript -e 'install.packages(c("reticulate", "magrittr", "dplyr", "tidyr", "tibble", "jsonlite", "stringr", "arrow", "readr", "reshape2", "lubridate", "purrr", "ndjson"))'
RUN Rscript -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'

USER root

EXPOSE 8787

CMD ["/init"]
