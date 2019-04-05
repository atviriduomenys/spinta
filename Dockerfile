FROM python:3.7-alpine as builder

WORKDIR /opt/

COPY requirements-dev.txt .

RUN apk update && \
    apk add --virtual build-deps python-dev build-base && \
    apk add postgresql-dev libxslt-dev libxml2-dev

RUN pip wheel --wheel-dir=/tmp -r requirements-dev.txt


FROM python:3.7-alpine

WORKDIR /opt/spinta

RUN apk add --no-cache libpq libxml2 libxslt

COPY --from=builder /tmp/*.whl /tmp/wheels/

COPY . .

RUN pip install --no-index -f /tmp/wheels -r requirements-dev.txt -e .

CMD ["uvicorn", "spinta.scripts.spinta_backend:app", "--debug", "--host", "0.0.0.0"]

EXPOSE 8000
