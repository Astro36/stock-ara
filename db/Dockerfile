FROM postgres:16-bookworm

RUN apt-get update && apt-get install -y wget

RUN echo "deb https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" | tee /etc/apt/sources.list.d/timescaledb.list && \
    wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add -

RUN apt-get update && \
    apt-get install -y timescaledb-2-postgresql-16 postgresql-16-pgvector && \
    rm -rf /var/lib/apt/lists/*
