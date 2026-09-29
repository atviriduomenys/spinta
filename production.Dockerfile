FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

COPY requirements /app/requirements
COPY startup.sh /app/startup.sh
RUN useradd -m -s /bin/bash spinta

RUN chmod +x /app/startup.sh
RUN chown -R spinta:spinta /app

ARG VERSION=latest-pre

WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq-dev \
        postgresql-client \
        default-libmysqlclient-dev \
        unixodbc \
        unixodbc-dev \
        libsqlite3-dev && \
    pip install --upgrade pip wheel setuptools && \
    pip install --require-hashes -r /app/requirements/spinta-${VERSION}.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER spinta

VOLUME /app/spinta_config
WORKDIR /app

EXPOSE 8000

CMD ["/app/startup.sh"]
