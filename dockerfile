FROM apache/airflow:2.8.1-python3.11

USER root

# -------------------------------
# System dependencies + Java
# -------------------------------
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk \
        curl \
        wget \
        gcc \
        g++ \
        python3-dev \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Install Apache Spark
# -------------------------------
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# -------------------------------
# Environment variables
# -------------------------------
# Updated JAVA_HOME to actual path on ARM Mac
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=/opt/airflow:/opt/airflow/bank_etl

# -------------------------------
# Python dependencies
# -------------------------------
USER airflow
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt

WORKDIR /opt/airflow
