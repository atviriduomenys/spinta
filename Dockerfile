FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY . /app/
WORKDIR /app

RUN rm entrypoint.sh

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
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction

