FROM rocker/geospatial:latest

# Install required R packages
RUN Rscript -e "install.packages(c('magrittr','purrr','dplyr','stringr','sf','arrow','here'), repos='https://cloud.r-project.org')"
RUN Rscript -e "install.packages('remotes', repos='https://cloud.r-project.org'); remotes::install_github('GOFUVI/S1OCN')"

WORKDIR /app
COPY download_SAR.R /app/

# Default entrypoint executes the download_SAR R script
ENTRYPOINT ["Rscript", "/app/download_SAR.R"]