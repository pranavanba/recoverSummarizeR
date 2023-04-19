FROM rocker/rstudio:latest

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y git

USER rstudio

RUN git clone https://github.com/pranavanba/convert2i2b2.git && \
    chown -R rstudio:rstudio /home/recover/convert2i2b2

RUN Rscript -e 'install.packages(c("PACKAGE1", "PACKAGE2"))'

USER root

EXPOSE 8787

CMD ["/init"]
