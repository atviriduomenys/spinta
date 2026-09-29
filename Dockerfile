FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

COPY . /app/
RUN useradd -m -s /bin/bash spinta

RUN chmod +x /app/entrypoint-internal.sh
RUN chmod +x /app/entrypoint-external.sh
RUN chown -R spinta:spinta /app

WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        cmake \
        make \
        git \
        libboost-all-dev \
        libsnappy-dev \
        libgflags-dev \
        libgoogle-glog-dev && \
    pip install --upgrade pip wheel setuptools && \
    pip install --upgrade poetry && \
    su - spinta -c "cd /app; poetry install --no-interaction --all-extras --no-cache" && \
    apt-get purge -y \
        gcc \
        g++ \
        cmake && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER spinta
WORKDIR /app

EXPOSE 8000
