FROM eclipse-temurin:11-jre AS jre
FROM apache/airflow:2.11.2-python3.11

USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir --default-timeout=100 \
    "apache-airflow-providers-apache-spark==4.11.2" \
    "apache-airflow-providers-docker==3.14.0" \
    "pyspark==3.5.8"
