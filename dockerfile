FROM apache/airflow:2.3.0-python3.8

USER root

# 1. Cài thư viện Python
COPY requirements.txt /tmp/requirements.txt

# 2. Cài Apache Spark
ENV SPARK_VERSION=3.4.1 \
    HADOOP_VERSION=3

RUN apt-get update && apt-get install -y curl openjdk-11-jdk && \
    curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
