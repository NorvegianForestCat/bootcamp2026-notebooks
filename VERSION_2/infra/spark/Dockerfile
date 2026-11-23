FROM apache/spark:3.5.8

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3.11 && \
    rm -rf /var/lib/apt/lists/*
USER spark
