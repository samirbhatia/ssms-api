FROM rocker/r-ver:4.5.2

# System libraries required by R packages
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    build-essential \
    zlib1g-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

# Install all required R packages
RUN Rscript -e "install.packages(c( \
  'plumber', \
  'DBI', \
  'duckdb', \
  'digest', \
  'jsonlite', \
  'openssl', \
  'httr' \
), repos='https://cloud.r-project.org')"

WORKDIR /app

COPY api.R /app/api.R
COPY index.html /app/index.html

# Render sets PORT automatically (usually 10000)
CMD ["Rscript", "-e", "pr <- plumber::plumb('/app/api.R'); pr$run(host='0.0.0.0', port=as.integer(Sys.getenv('PORT','10000')))"]
