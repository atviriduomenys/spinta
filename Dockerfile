FROM python:3.7-alpine

WORKDIR /opt/spinta

COPY . .

RUN wget -O /usr/local/bin/wait-for https://raw.githubusercontent.com/eficode/wait-for/master/wait-for \
    && chmod +x /usr/local/bin/wait-for

RUN apk update && \
    apk add --virtual build-deps g++ gcc python-dev musl-dev build-base abuild binutils binutils-doc gcc-doc && \
    apk add postgresql-dev libxslt-dev libxml2-dev libresolv-dev

RUN make

CMD ["wait-for", "db:5432", "-t", "60", "--", "env/bin/uvicorn", "spinta.asgi:app", "--debug"]

EXPOSE 8000
