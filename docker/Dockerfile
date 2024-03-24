FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

USER airflow

#WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt