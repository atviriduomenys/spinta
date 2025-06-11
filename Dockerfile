FROM python:3.9-alpine as rmanifests

WORKDIR /tmp

RUN apk add git
RUN git clone https://github.com/atviriduomenys/demo-saltiniai.git

# Catch till first manifest will be defined
RUN mkdir -p demo-saltiniai/manifests


FROM python:3.9-slim as base

EXPOSE 8000
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    cmake \
    make \
    libboost-all-dev \
    libsnappy-dev \
    libgflags-dev \
    libgoogle-glog-dev

RUN pip install -U pip wheel setuptools
RUN pip install -U poetry

COPY . /app/

RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --all-extras

RUN chmod +x /app/entrypoint.sh

FROM base as manifests

WORKDIR /tmp
COPY --from=rmanifests /tmp/demo-saltiniai/manifests /tmp/manifests

RUN spinta upgrade
RUN ls /tmp/manifests | xargs spinta copy -o /tmp/manifest.csv


FROM base as build
COPY --from=manifests /tmp/manifest.csv /app/manifest.csv
