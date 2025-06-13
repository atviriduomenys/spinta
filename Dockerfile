FROM python:3.9-slim

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

EXPOSE 8000
