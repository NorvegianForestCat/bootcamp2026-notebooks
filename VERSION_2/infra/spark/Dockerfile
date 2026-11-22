FROM apache/spark:3.5.8

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3.11 openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER spark
