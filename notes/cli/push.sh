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
    id          INTEGER PRIMARY KEY,
    name        TEXT
);
CREATE TABLE IF NOT EXISTS cities (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    country_id  INTEGER,
    FOREIGN KEY (country_id) REFERENCES countries (id)
);

INSERT INTO countries VALUES (1, 'Lithuania');
INSERT INTO cities VALUES (1, 'Vilnius', 1);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
#| +----+-----------+
#| | id |   name    |
#| +----+-----------+
#| | 1  | Lithuania |
#| +----+-----------+
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+------------+
#| | id |  name   | country_id |
#| +----+---------+------------+
#| | 1  | Vilnius | 1          |
#| +----+---------+------------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | source     | prepare | access
$DATASET                 |         |         |            |         |
  | db                   | sql     |         | sqlite:///$BASEDIR/db.sqlite | |
  |   |   | Country      |         | id      | countries  |         |
  |   |   |   | id       | integer |         | id         |         | open
  |   |   |   | name     | string  |         | name       |         | open
  |   |   | City         |         | id      | cities     |         |
  |   |   |   | id       | integer |         | id         |         | open
  |   |   |   | name     | string  |         | name       |         | open
  |   |   |   | country  | ref     | Country | country_id |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Country?format(ascii)"
#|      _type                         _id                    _revision   id     name   
#| ====================================================================================
#| cli/push/Country   f2c62384-1591-416c-8ed6-40a89c682e0f   None        1    Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
#|     _type                       _id                    _revision   id    name                 country._id             
#| ======================================================================================================================
#| cli/push/City   820f9c83-8654-4a72-9c71-7958a492b41f   None        1    Vilnius   f2c62384-1591-416c-8ed6-40a89c682e0f

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client

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

http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 2/2 [00:00<00:00, 21.41it/s]

# Push state database tables should be updates to a new schema
sqlite3 $BASEDIR/push/localhost.db '.schema'
#| CREATE TABLE IF NOT EXISTS "cli/push/Country" (
#|     "id"            VARCHAR NOT NULL PRIMARY KEY,
#|     "checksum"      VARCHAR,
#|     "pushed"        DATETIME,
#|     "revision"      VARCHAR,
#|     "error"         BOOLEAN
#| );
#| CREATE TABLE IF NOT EXISTS "cli/push/City" (
#|     "id"            VARCHAR NOT NULL PRIMARY KEY,
#|     "checksum"      VARCHAR,
#|     "pushed"        DATETIME,
#|     "revision"      VARCHAR,
#|     "error"         BOOLEAN
#| );

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision                             id  name     
#| ----------------  ------------------------------------  ------------------------------------  --  ---------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  ea77fc88-3aa5-4aa0-8c13-56c8359b504d  1   Lithuania

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name     country._id                         
#| -------------  ------------------------------------  ------------------------------------  --  -------  ------------------------------------
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  58d7d711-acef-4760-95c9-c453c2587815  1   Vilnius  f2c62384-1591-416c-8ed6-40a89c682e0f

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
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 2023-03-07 17:01:53.475036 | 58d7d711-acef-4760-95c9-c453c2587815 | 0     |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 2023-03-07 17:01:53.474127 | ea77fc88-3aa5-4aa0-8c13-56c8359b504d | 0     |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+

# Delete Vilnius from source database
sqlite3 $BASEDIR/db.sqlite "DELETE FROM cities WHERE id = 1;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Push again, Vilnius should be deleted from target too.
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 1/1 [00:00<00:00, 106.44it/s]

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
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 2023-03-07 17:42:47.467519 | ea77fc88-3aa5-4aa0-8c13-56c8359b504d | 0     |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision                             id  name     
#| ----------------  ------------------------------------  ------------------------------------  --  ---------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  ea77fc88-3aa5-4aa0-8c13-56c8359b504d  1   Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
# Vilnius was deleted, indeed.

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name   
#| ------  --------------------------  ------------------------------------  -------
#| insert  2023-03-07T15:01:53.463447  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
#| delete  2023-03-07T15:42:47.472209  820f9c83-8654-4a72-9c71-7958a492b41f  ∅

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
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2023-03-07 17:51:19.610534 | 857cd064-6232-4573-bf5a-220fc39da09a | 0     |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| |                  id                  |                 checksum                 |           pushed           |               revision               | error |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 2023-03-07 17:51:19.591352 | ea77fc88-3aa5-4aa0-8c13-56c8359b504d | 0     |
#| +--------------------------------------+------------------------------------------+----------------------------+--------------------------------------+-------+

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type             _id                                   _revision                             id  name     
#| ----------------  ------------------------------------  ------------------------------------  --  ---------
#| cli/push/Country  f2c62384-1591-416c-8ed6-40a89c682e0f  ea77fc88-3aa5-4aa0-8c13-56c8359b504d  1   Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
#|_type          _id                                   _revision                             id  name    country._id                         
#|-------------  ------------------------------------  ------------------------------------  --  ------  ------------------------------------
#|cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  857cd064-6232-4573-bf5a-220fc39da09a  2   Kaunas  f2c62384-1591-416c-8ed6-40a89c682e0f

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
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 68dded8b-ecd5-49d3-ab44-6ce82b565904 | 2023-03-07 15:43:24.300810 | 0     |
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 3cf8ec5b-1ab8-4787-a55e-5c8734930238 | 2023-03-07 15:43:24.308148 | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name    country._id                         
#| -------------  ------------------------------------  ------------------------------------  --  ------  ------------------------------------
#| cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  68dded8b-ecd5-49d3-ab44-6ce82b565904  2   Kaunas  f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  3cf8ec5b-1ab8-4787-a55e-5c8734930238  1   Vilniu  f2c62384-1591-416c-8ed6-40a89c682e0f\
#|                                                                                                s

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name   
#| ------  --------------------------  ------------------------------------  -------
#| insert  2023-03-07T13:20:38.558093  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
#| delete  2023-03-07T13:23:51.432334  820f9c83-8654-4a72-9c71-7958a492b41f  ∅
#| insert  2023-03-07T13:40:18.198174  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  Kaunas
#| insert  2023-03-07T13:43:24.304510  820f9c83-8654-4a72-9c71-7958a492b41f  Vilnius
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
#| ERROR: Error when sending and receiving data.                                                                                                                                
#| Server response (status=404):
#| 
#|         <!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
#|         <html>
#|             <head>
#|                 <title>Error: 404 Not Found</title>
#|                 <style type="text/css">
#|                   html {background-color: #eee; font-family: sans;}
#|                   body {background-color: #fff; border: 1px solid #ddd;
#|                         padding: 15px; margin: 15px;}
#|                   pre {background-color: #eee; border: 1px solid #ddd; padding: 5px;}
#|                 </style>
#|             </head>
#|             <body>
#|                 <h1>Error: 404 Not Found</h1>
#|                 <p>Sorry, the requested URL <tt>&#039;http://localhost:8000/&#039;</tt>
#|                    caused an error:</p>
#|                 <pre>Not found: &#039;/&#039;</pre>
#|             </body>
#|         </html>
#| Traceback (most recent call last):
#|   File "spinta/cli/push.py", line 343, in _push_rows
#|     next(rows)
#|   File "spinta/cli/push.py", line 807, in _save_push_state
#|     for row in rows:
#|   File "spinta/cli/push.py", line 412, in _push_to_remote_spinta
#|     for row in rows:
#|   File "spinta/cli/push.py", line 379, in _prepare_rows_for_push
#|     for row in rows:
#|   File "spinta/cli/push.py", line 776, in _check_push_state
#|     for row in group:
#|   File "tqdm/std.py", line 1195, in __iter__
#|     for obj in iterable:
#|   File "spinta/cli/push.py", line 323, in _read_rows
#|     yield from _get_rows_with_errors(
#|   File "spinta/cli/push.py", line 914, in _get_rows_with_errors
#|     yield from _prepare_rows_with_errors(
#|   File "spinta/cli/push.py", line 938, in _prepare_rows_with_errors
#|     data = commands.getone(context, model, model.backend, id_=_id)
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for getone: <Context, Model, Sql>
# FIXME: It seems, that we need to save whole object in the push state in case
#        of error, because not all dataset backends will support getone.

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 68dded8b-ecd5-49d3-ab44-6ce82b565904 | 2023-03-07 15:52:32.224942 | 0     |
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 3cf8ec5b-1ab8-4787-a55e-5c8734930238 | 2023-03-07 15:52:32.224348 | 0     |
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b |                                      | 2023-03-07 15:52:32.228806 | 1     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+

# Bring a functioning server back online
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Try to push and check if errored row is retried
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| Traceback (most recent call last):
#|   File "spinta/cli/push.py", line 343, in _push_rows
#|     next(rows)
#|   File "spinta/cli/push.py", line 807, in _save_push_state
#|     for row in rows:
#|   File "spinta/cli/push.py", line 412, in _push_to_remote_spinta
#|     for row in rows:
#|   File "spinta/cli/push.py", line 379, in _prepare_rows_for_push
#|     for row in rows:
#|   File "spinta/cli/push.py", line 776, in _check_push_state
#|     for row in group:
#|   File "tqdm/std.py", line 1195, in __iter__
#|     for obj in iterable:
#|   File "spinta/cli/push.py", line 323, in _read_rows
#|     yield from _get_rows_with_errors(
#|   File "spinta/cli/push.py", line 914, in _get_rows_with_errors
#|     yield from _prepare_rows_with_errors(
#|   File "spinta/cli/push.py", line 938, in _prepare_rows_with_errors
#|     data = commands.getone(context, model, model.backend, id_=_id)
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for getone: <Context, Model, Sql>
# FIXME: Yes, same error as above, we need to save objects on errors in order
#        to be able to retry them later. After each retry object should be
#        deleted and error flag cleard.

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 68dded8b-ecd5-49d3-ab44-6ce82b565904 | 2023-03-07 16:11:05.379377 | 0     |
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 3cf8ec5b-1ab8-4787-a55e-5c8734930238 | 2023-03-07 16:11:05.378708 | 0     |
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b |                                      | 2023-03-07 16:11:05.379801 | 1     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+
# FIXME: It seems, that nothing has changed.

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type          _id                                   _revision                             id  name    country._id                         
#| -------------  ------------------------------------  ------------------------------------  --  ------  ------------------------------------
#| cli/push/City  2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd  68dded8b-ecd5-49d3-ab44-6ce82b565904  2   Kaunas  f2c62384-1591-416c-8ed6-40a89c682e0f
#| cli/push/City  820f9c83-8654-4a72-9c71-7958a492b41f  3cf8ec5b-1ab8-4787-a55e-5c8734930238  1   Vilniu  f2c62384-1591-416c-8ed6-40a89c682e0f\
#|                                                                                                s
# FIXME: Klaipėda should be in the list of cieties, but is not.
