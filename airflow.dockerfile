# syntax=docker/dockerfile:1
FROM apache/airflow:3.0.6

# switch to root
USER root

# Install additionnal tools
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  unzip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# switch to airflow
USER airflow