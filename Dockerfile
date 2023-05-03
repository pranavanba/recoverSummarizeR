FROM rocker/rstudio:latest

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y git python3 python3-pip python3-venv curl

USER rstudio

RUN Rscript -e 'install.packages("synapser", repos = c("http://ran.synapse.org", "http://cran.fhcrc.org"))'
RUN Rscript -e 'install.packages("devtools")'
RUN Rscript -e 'require(devtools)'
RUN Rscript -e 'install_github("Sage-Bionetworks/recoverSummarizeR")'

USER root

EXPOSE 8787

CMD ["/init"]
