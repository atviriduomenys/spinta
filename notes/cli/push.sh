# 2023-02-21 14:23

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=cli/push
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    continent_id INTEGER
);
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    country_id   INTEGER,
    FOREIGN KEY  (country_id) REFERENCES countries (id)
);

INSERT INTO countries VALUES (1, 'Lithuania', 42);
INSERT INTO cities VALUES (1, 'Vilnius', 1);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
#| +----+-----------+--------------+
#| | id |   name    | continent_id |
#| +----+-----------+--------------+
#| | 1  | Lithuania | 42           |
#| +----+-----------+--------------+
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+------------+
#| | id |  name   | country_id |
#| +----+---------+------------+
#| | 1  | Vilnius | 1          |
#| +----+---------+------------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref                          | source       | prepare | level | access
datasets/external         |         |                              |              |         |       |
  |   |   | Continent     |         | id                           |              |         | 4     |
  |   |   |   | id        | integer |                              |              |         | 4     | open
  |   |   |   | name      | string  |                              |              |         | 4     | open
$DATASET                  |         |                              |              |         |       |
  | db                    | sql     |                              | sqlite:///$BASEDIR/db.sqlite | | |
  |   |   | Country       |         | id                           | countries    |         | 4     |
  |   |   |   | id        | integer |                              | id           |         | 4     | open
  |   |   |   | name      | string  |                              | name         |         | 4     | open
  |   |   |   | continent | ref     | /datasets/external/Continent | continent_id |         | 3     | open
  |   |   | City          |         | id                           | cities       |         | 4     |
  |   |   |   | id        | integer |                              | id           |         | 4     | open
  |   |   |   | name      | string  |                              | name         |         | 4     | open
  |   |   |   | country   | ref     | Country                      | country_id   |         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/datasets/external/Continent"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "ProgrammingError",
#|             "message": "
#|                 (psycopg2.errors.UndefinedTable)
#|                     relation "datasets/external/Continent" does not exist
#|                 LINE 2
#|                     FROM "datasets/external/Continent"
#|                 SQL:
#|                     SELECT
#|                         "datasets/external/Continent"._id,
#|                         "datasets/external/Continent"._revision,
#|                         "datasets/external/Continent".id,
#|                         "datasets/external/Continent".name
#|                     FROM "datasets/external/Continent"
#|             "
#|         }
#|     ]
#| }
# TODO: Show an 400 error, explaining, that source is not set.

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision  id  name       continent.id
#| ----------------  ------------------------------------  ---------  --  ---------  ------------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  ∅          1   Lithuania  42
http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision  id  name     country._id                         
#| -------------  ------------------------------------  ---------  --  -------  ------------------------------------
#| cli/push/City  31bfc6b7-7ad5-48ec-83e9-0e6841b5dd43  ∅          1   Vilnius  2b7f8f23-89d7-479c-8b4e-f86612d608e2

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/postgres.sh         Show tables

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|     Column     |            Type             | Collation | Nullable | Default 
#| ---------------+-----------------------------+-----------+----------+---------
#|  continent.id  | integer                     |           |          |

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

# Create push state table using an old schema
rm $BASEDIR/push/localhost.db
sqlite3 $BASEDIR/push/localhost.db <<EOF
CREATE TABLE IF NOT EXISTS "$DATASET/Country" (
    id          VARCHAR NOT NULL PRIMARY KEY,
    rev         VARCHAR,
    pushed      DATETIME
);
CREATE TABLE IF NOT EXISTS "$DATASET/City" (
    id          VARCHAR NOT NULL PRIMARY KEY,
    rev         VARCHAR,
    pushed      DATETIME
);
EOF
sqlite3 $BASEDIR/push/localhost.db '.schema'

# Delete all data from internal database, to start with a clean state.
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/datasets/external/Continent?format(ascii)"
http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| Traceback (most recent call last):
#|   File "multipledispatch/dispatcher.py", line 269, in __call__
#|     func = self._cache[types]
#| KeyError: (<class 'spinta.backends.postgresql.commands.query.PgQueryBuilder'>, <class 'NoneType'>)
#|
#| During handling of the above exception, another exception occurred:
#|
#| Traceback (most recent call last):
#|   File "/spinta/core/ufuncs.py", line 216, in call
#|     return ufunc(self, *args, **kwargs)
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for select: <PgQueryBuilder, NoneType>
#|
#| During handling of the above exception, another exception occurred:
#|
#| Traceback (most recent call last):
#|   File "/spinta/cli/helpers/data.py", line 51, in count_rows
#|     count = _get_row_count(context, model)
#|   File "/spinta/cli/helpers/data.py", line 36, in _get_row_count
#|     for data in stream:
#|   File "/spinta/backends/postgresql/commands/read.py", line 51, in getall
#|     expr = env.resolve(query)
#|   File "/spinta/backends/postgresql/commands/query.py", line 285, in select
#|     selected = env.call('select', arg)
#|   File "/spinta/core/ufuncs.py", line 218, in call
#|     raise UnknownMethod(expr=str(Expr(name, *args, **kwargs)), name=name)
#| spinta.exceptions.UnknownMethod: Unknown method 'select' with args select(null).
#|   Context:
#|     expr: select(null)
#|     name: select
# TODO: https://github.com/atviriduomenys/spinta/issues/394

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost -d $DATASET
#| PUSH: 100%|#| 2/2 [00:00<00:00, 445.04it/s]

# Push state database tables should be updates to a new schema
sqlite3 $BASEDIR/push/localhost.db '.schema'
#| CREATE TABLE IF NOT EXISTS "cli/push/Country" (
#|     id        VARCHAR NOT NULL PRIMARY KEY,
#|     checksum  VARCHAR,
#|     pushed    DATETIME,
#|     revision  VARCHAR,
#|     error     BOOLEAN,
#|     data      TEXT
#| );
#| CREATE TABLE IF NOT EXISTS "cli/push/City" (
#|     id        VARCHAR NOT NULL PRIMARY KEY,
#|     checksum  VARCHAR,
#|     pushed    DATETIME,
#|     revision  VARCHAR,
#|     error     BOOLEAN,
#|     data      TEXT
#| );
#| CREATE TABLE IF NOT EXISTS "datasets/external/Continent" (
#|     id        VARCHAR NOT NULL,
#|     checksum  VARCHAR,
#|     revision  VARCHAR,
#|     pushed    DATETIME,
#|     error     BOOLEAN,
#|     data      TEXT,
#|     PRIMARY KEY (id)
#| );

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision                             id  name
#| ----------------  ------------------------------------  ------------------------------------  --  ---------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  dee21e6a-afe4-4b00-8333-9dd85afa951b  1   Lithuania

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name     country._id
#| -------------  ------------------------------------  ------------------------------------  --  -------  ------------------------------------
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  fc41d0f5-a3b4-4079-b32e-f6fbb4c5c809  1   Vilnius  f2c62384-1591-416c-8ed6-40a89c682e0f

sqlite3 $BASEDIR/keymap.db .tables
sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| +--------------------------------------+------------------------------------------+-------+------------+
sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/push/localhost.db .tables
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 2023-03-13 11:48:26.655362 | fc41d0f5-a3b4-4079-b32e-f6fbb4c5c809 | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 2023-03-13 11:48:26.654330 | dee21e6a-afe4-4b00-8333-9dd85afa951b | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+

# Delete Vilnius from source database
sqlite3 $BASEDIR/db.sqlite "DELETE FROM cities WHERE id = 1;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Push again, Vilnius should be deleted from target too.
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|############| 1/1 [00:00<00:00, 277.29it/s]
#| PUSH DELETED: 100%|####| 1/1 [00:00<00:00, 701.04it/s]

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| +--------------------------------------+------------------------------------------+-------+------------+
sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
# (empty table)
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 2023-03-13 11:53:30.480518 | dee21e6a-afe4-4b00-8333-9dd85afa951b | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision                             id  name
#| ----------------  ------------------------------------  ------------------------------------  --  ---------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  dee21e6a-afe4-4b00-8333-9dd85afa951b  1   Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
# Vilnius was deleted, indeed.

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name
#| ------  --------------------------  ------------------------------------  -------
#| insert  2023-03-13T09:48:26.643939  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
#| delete  2023-03-13T09:53:30.487053  820f9c83-8654-4a72-9c71-7958a492b41f  ∅

# Add new record to the source database.
sqlite3 $BASEDIR/db.sqlite "INSERT INTO cities VALUES (2, 'Kaunas', 1);"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+--------+------------+
#| | id |  name  | country_id |
#| +----+--------+------------+
#| | 2  | Kaunas | 1          |
#| +----+--------+------------+

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 2/2 [00:00<00:00, 188.70it/s]

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| +--------------------------------------+------------------------------------------+-------+------------+
sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | c4ea21bb365bbeeaf5f2c654883e56d11e43c44e |       | 02         |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2023-03-13 11:55:59.027458 | 0d87e54b-a29c-42f6-8bbe-8c9984bab00d | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 2023-03-13 11:55:59.019106 | dee21e6a-afe4-4b00-8333-9dd85afa951b | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision                             id  name
#| ----------------  ------------------------------------  ------------------------------------  --  ---------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  dee21e6a-afe4-4b00-8333-9dd85afa951b  1   Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name    country._id
#| -------------  ------------------------------------  ------------------------------------  --  ------  ------------------------------------
#| cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  0d87e54b-a29c-42f6-8bbe-8c9984bab00d  2   Kaunas  f2c62384-1591-416c-8ed6-40a89c682e0f

# Reindtoruce Vilnius object back to source database.
sqlite3 $BASEDIR/db.sqlite "INSERT INTO cities VALUES (1, 'Vilnius', 1);"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+------------+
#| | id |  name   | country_id |
#| +----+---------+------------+
#| | 1  | Vilnius | 1          |
#| | 2  | Kaunas  | 1          |
#| +----+---------+------------+

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 3/3 [00:00<00:00, 264.17it/s]

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2023-03-13 11:58:07.082583 | 0d87e54b-a29c-42f6-8bbe-8c9984bab00d | 0     |      |
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 2023-03-13 11:58:07.089081 | 7e054b5d-06b8-43ae-a891-2bf8840c0a1d | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name    country._id
#| -------------  ------------------------------------  ------------------------------------  --  ------  ------------------------------------
#| cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  0d87e54b-a29c-42f6-8bbe-8c9984bab00d  2   Kaunas  f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  7e054b5d-06b8-43ae-a891-2bf8840c0a1d  1   Vilniu  f2c62384-1591-416c-8ed6-40a89c682e0f\
#|                                                                                                s

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name
#| ------  --------------------------  ------------------------------------  -------
#| insert  2023-03-13T09:48:26.643939  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
#| delete  2023-03-13T09:53:30.487053  820f9c83-8654-4a72-9c71-7958a492b41f  ∅
#| insert  2023-03-13T09:55:59.024079  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  Kaunas
#| insert  2023-03-13T09:58:07.085500  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
# Vilnius was reintroduced correctly


# Run server that response with error to every request except /auth/token
test -n "$PID" && kill $PID
poetry run pip install bottle
poetry run python &>> $BASEDIR/bottle.log <<'EOF' &
from bottle import route, request, response, run
@route('/auth/token', 'POST')
def token():
    return {'access_token': 'TOKEN'}
@route('/<path:path>', method=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'])
def info(path):
    response.status = 404
    return {
        'method': request.method,
        'path': request.path,
        'query': dict(request.query),
        'headers': dict(request.headers),
        'body': request.json or dict(request.forms or {}),
    }
run(port=8000, debug=True)
EOF
PID=$!
tail -50 $BASEDIR/bottle.log

sqlite3 $BASEDIR/db.sqlite "INSERT INTO cities VALUES (3, 'Klaipėda', 1);"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Try to push with a 404 response code
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|#######| 4/4 [00:00<00:00, 703.68it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):
#| RETRY #1: 100%|###| 1/1 [00:00<00:00, 463.05it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):
#| RETRY #2: 100%|###| 1/1 [00:00<00:00, 532.47it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):
#| RETRY #3: 100%|###| 1/1 [00:00<00:00, 647.77it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):
#| RETRY #4: 100%|###| 1/1 [00:00<00:00, 627.23it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):
#| RETRY #5: 100%|###| 1/1 [00:00<00:00, 691.79it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |                             data                                |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2023-03-13 12:00:21.332956 | 0d87e54b-a29c-42f6-8bbe-8c9984bab00d | 0     |                                                                 |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 2023-03-13 12:00:21.332354 | 7e054b5d-06b8-43ae-a891-2bf8840c0a1d | 0     |                                                                 |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b | 2023-03-13 12:00:21.358759 |                                      | 1     | {                                                               |
#| |                                      |                                          |                            |                                      |       |     "_type": "cli/push/City",                                   |
#| |                                      |                                          |                            |                                      |       |     "_id": "c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971",              |
#| |                                      |                                          |                            |                                      |       |     "id": 3,                                                    |
#| |                                      |                                          |                            |                                      |       |     "name": "Klaipėda",                                         |
#| |                                      |                                          |                            |                                      |       |     "country": {"_id": "f2c62384-1591-416c-8ed6-40a89c682e0f"}, |
#| |                                      |                                          |                            |                                      |       |     "_op": "insert"                                             |
#| |                                      |                                          |                            |                                      |       | }                                                               |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+


poetry run spinta push $BASEDIR/manifest.csv -o test@localhost --max-errors 2
#| PUSH: 100%|#########| 4/4 [00:00<00:00, 681.75it/s]
#| RETRY #1: 100%|#####| 1/1 [00:00<00:00, 342.53it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):
#| RETRY #2: 100%|#####| 1/1 [00:00<00:00, 610.44it/s]
#|     ERROR: Error when sending and receiving data.
#|     Server response (status=404):

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |                             data                                |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2023-03-13 12:00:21.332956 | 0d87e54b-a29c-42f6-8bbe-8c9984bab00d | 0     |                                                                 |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 2023-03-13 12:00:21.332354 | 7e054b5d-06b8-43ae-a891-2bf8840c0a1d | 0     |                                                                 |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b | 2023-03-13 12:00:21.358759 |                                      | 1     | {                                                               |
#| |                                      |                                          |                            |                                      |       |     "_type": "cli/push/City",                                   |
#| |                                      |                                          |                            |                                      |       |     "_id": "c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971",              |
#| |                                      |                                          |                            |                                      |       |     "id": 3,                                                    |
#| |                                      |                                          |                            |                                      |       |     "name": "Klaipėda",                                         |
#| |                                      |                                          |                            |                                      |       |     "country": {"_id": "f2c62384-1591-416c-8ed6-40a89c682e0f"}, |
#| |                                      |                                          |                            |                                      |       |     "_op": "insert"                                             |
#| |                                      |                                          |                            |                                      |       | }                                                               |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+-----------------------------------------------------------------+


# Bring a functioning server back online
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log


# Try to push and check if errored row is retried
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|#########| 4/4 [00:00<00:00, 678.66it/s]
#| RETRY #1: 100%|#####| 1/1 [00:00<00:00, 82.53it/s]
# TODO: I think, we need to retry first, if we have errors, or at least,
#       errored rows, should be included in the first push.

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error | data |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2023-03-13 12:21:16.840625 | 0d87e54b-a29c-42f6-8bbe-8c9984bab00d | 0     |      |
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 2023-03-13 12:21:16.839987 | 7e054b5d-06b8-43ae-a891-2bf8840c0a1d | 0     |      |
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b | 2023-03-13 12:21:16.864241 | 057c6f7a-d32c-46bb-9a70-c23deb6fbfa4 | 0     |      |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+------+

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name     country._id
#| -------------  ------------------------------------  ------------------------------------  --  -------  ------------------------------------
#| cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  0d87e54b-a29c-42f6-8bbe-8c9984bab00d  2   Kaunas   f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  7e054b5d-06b8-43ae-a891-2bf8840c0a1d  1   Vilnius  f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971  057c6f7a-d32c-46bb-9a70-c23deb6fbfa4  3   Klaipėd  f2c62384-1591-416c-8ed6-40a89c682e0f\
#|                                                                                                a


# Try to update a record.
sqlite3 $BASEDIR/db.sqlite "UPDATE cities SET name = 'Kaunas (updated)' WHERE id = 2;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+------------------+------------+
#| | id |       name       | country_id |
#| +----+------------------+------------+
#| | 1  | Vilnius          | 1          |
#| | 2  | Kaunas (updated) | 1          |
#| | 3  | Klaipėda         | 1          |
#| +----+------------------+------------+

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 4/4 [00:00<00:00, 710.63it/s]

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name        country._id
#| -------------  ------------------------------------  ------------------------------------  --  ----------  ------------------------------------
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  7e054b5d-06b8-43ae-a891-2bf8840c0a1d  1   Vilnius     f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971  057c6f7a-d32c-46bb-9a70-c23deb6fbfa4  3   Klaipėda    f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  81f696e7-3b8d-4edc-b697-8d5d30ea68f8  2   Kaunas (up  f2c62384-1591-416c-8ed6-40a89c682e0f\
#|                                                                                                dated)

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name
#| ------  --------------------------  ------------------------------------  ----------
#| insert  2023-03-13T09:48:26.643939  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
#| delete  2023-03-13T09:53:30.487053  820f9c83-8654-4a72-9c71-7958a492b41f  ∅
#| insert  2023-03-13T09:55:59.024079  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  Kaunas
#| insert  2023-03-13T09:58:07.085500  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
#| insert  2023-03-13T10:21:16.857179  c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971  Klaipėda
#| patch   2023-03-13T10:24:42.454746  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  Kaunas (up\
#|                                                                           dated)
# TODO: Last line should not be wrapped.
#       https://github.com/atviriduomenys/spinta/issues/389
