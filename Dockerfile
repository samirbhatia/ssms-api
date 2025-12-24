FROM rocker/r-ver:4.5.2

RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    build-essential \
    zlib1g-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

RUN Rscript -e "install.packages(c('plumber','DBI','duckdb'), repos='https://cloud.r-project.org')"

WORKDIR /app

COPY api.R /app/api.R
COPY index.html /app/index.html

CMD ["Rscript", "-e", "pr <- plumber::plumb('/app/api.R'); pr$run(host='0.0.0.0', port=8000)"]
