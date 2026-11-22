FROM apache/airflow:2.11.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir --default-timeout=100 \
    "apache-airflow-providers-apache-spark==4.11.2" \
    "apache-airflow-providers-docker==3.14.0" \
    "pyspark==3.5.8"
