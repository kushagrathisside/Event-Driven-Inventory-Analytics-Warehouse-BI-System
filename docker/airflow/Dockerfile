FROM apache/airflow:2.8.1-python3.11

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ARG http_proxy
ARG https_proxy
ARG no_proxy

USER root
RUN HTTP_PROXY="${HTTP_PROXY}" HTTPS_PROXY="${HTTPS_PROXY}" NO_PROXY="${NO_PROXY}" \
    http_proxy="${http_proxy}" https_proxy="${https_proxy}" no_proxy="${no_proxy}" \
    apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN HTTP_PROXY="${HTTP_PROXY}" HTTPS_PROXY="${HTTPS_PROXY}" NO_PROXY="${NO_PROXY}" \
    http_proxy="${http_proxy}" https_proxy="${https_proxy}" no_proxy="${no_proxy}" \
    pip install --no-cache-dir -r /tmp/requirements.txt
