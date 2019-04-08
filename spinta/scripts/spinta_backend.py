#!/usr/local/bin/python

# Helper script to check for postgres connection before running the app

import time

import psycopg2

from spinta.config import get_config


def check_postgres_connection(config):
    psql_dsn = config['backends']['default']['dsn']

    try:
        conn = psycopg2.connect(psql_dsn)
    except psycopg2.OperationalError:
        return False
    else:
        conn.close()
        return True

config = get_config()

# Wait while PostgreSQL is up.
for i in range(1, 31):
    if check_postgres_connection(config):
        break
    time.sleep(1)
    print("Waiting for PostgreSQL (%s)" % i)
else:
    print("Error: Can't connect to %s database." % (psql.dsn))
    sys.exit(1)


from spinta.asgi import app
