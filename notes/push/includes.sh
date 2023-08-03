# 2023-05-12 13:28

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/includes
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE cities (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    country     TEXT   
);
INSERT INTO cities VALUES (1, 'Vilnius', 'lt');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities"
#| +----+---------+---------+
#| | id |  name   | country |
#| +----+---------+---------+
#| | 1  | Vilnius | lt      |
#| +----+---------+---------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref                         | source   | level | access
$DATASET                 |         |                             |          |       |
  | db                   | sql     |                             | sqlite:///$BASEDIR/db.sqlite | |
  |   |   | City         |         | id                          | cities   | 4     |
  |   |   |   | id       | integer |                             | id       | 4     | open
  |   |   |   | name     | string  |                             | name     | 2     | open
  |   |   |   | country  | ref     | /$DATASET/countries/Country | country  | 3     | open
  |   |   |   |          |         |                             |          |       |
$DATASET/countries       |         |                             |          |       |
  |   |   | Country      |         | code                        |          | 4     |
  |   |   |   | code     | string  |                             |          | 4     | open
  |   |   |   | name     | string  |                             |          | 2     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client


http GET "$SERVER/$DATASET/City?limit(5)&format(ascii)"
#| _type               _id                                   _revision  _base  id  name     country.code
#| ------------------  ------------------------------------  ---------  -----  --  -------  ------------
#| push/includes/City  1cacb9c9-4350-4df2-8b5b-678c5669d731  ∅          ∅      1   Vilnius  lt

http GET "$SERVER/$DATASET/countries/Country?format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "ProgrammingError",
#|             "message": "
#|                 (psycopg2.errors.UndefinedTable)
#|                     relation push/includes/countries/Country does not exist
#| 
#|                 LINE 2:
#|                     FROM push/includes/countries/Country
#| 
#|                 SQL:
#|                     SELECT
#|                         push/includes/countries/Country._id,
#|                         push/includes/countries/Country._revision,
#|                         push/includes/countries/Country.code,
#|                         push/includes/countries/Country.name
#|                     FROM push/includes/countries/Country
#|             "
#|         }
#|     ]
#| }
# FIXME: In external mode, this should return 400 error, because source is not set, so we can't get data.

# notes/spinta/server.sh    Run migrations

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

rm $BASEDIR/push/localhost.db
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/$DATASET/City?limit(5)&format(ascii)"
http GET "$SERVER/$DATASET/countries/Country?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| 2023-08-03 17:21:56,569 ERROR: Error on _get_row_count({model.name}).
#| Traceback (most recent call last):
#|   File "multipledispatch/dispatcher.py", line 269, in __call__
#|     func = self._cache[types]
#|            ~~~~~~~~~~~^^^^^^^
#| KeyError: (<class 'spinta.backends.postgresql.commands.query.PgQueryBuilder'>, <class 'NoneType'>)
#| 
#| During handling of the above exception, another exception occurred:
#| 
#| Traceback (most recent call last):
#|   File "/home/sirex/dev/data/spinta/spinta/core/ufuncs.py", line 216, in call
#|     return ufunc(self, *args, **kwargs)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for select: <PgQueryBuilder, NoneType>
#| 
#| During handling of the above exception, another exception occurred:
#| 
#| Traceback (most recent call last):
#|   File "/home/sirex/dev/data/spinta/spinta/cli/helpers/data.py", line 51, in count_rows
#|     count = _get_row_count(context, model)
#|             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "/home/sirex/dev/data/spinta/spinta/cli/helpers/data.py", line 36, in _get_row_count
#|     for data in stream:
#|   File "/home/sirex/dev/data/spinta/spinta/backends/postgresql/commands/read.py", line 51, in getall
#|     expr = env.resolve(query)
#|            ^^^^^^^^^^^^^^^^^^
#|   File "/home/sirex/dev/data/spinta/spinta/core/ufuncs.py", line 242, in resolve
#|     return ufunc(self, expr)
#|            ^^^^^^^^^^^^^^^^^
#|   File "multipledispatch/dispatcher.py", line 278, in __call__
#|     return func(*args, **kwargs)
#|            ^^^^^^^^^^^^^^^^^^^^^
#|   File "/home/sirex/dev/data/spinta/spinta/backends/postgresql/commands/query.py", line 285, in select
#|     selected = env.call('select', arg)
#|                ^^^^^^^^^^^^^^^^^^^^^^^
#|   File "/home/sirex/dev/data/spinta/spinta/core/ufuncs.py", line 218, in call
#|     raise UnknownMethod(expr=str(Expr(name, *args, **kwargs)), name=name)
#| spinta.exceptions.UnknownMethod: Unknown method 'select' with args select(null).
#|   Context:
#|     expr: select(null)
#|     name: select
#| 
#| PUSH: 100%|#############| 1/1 [00:00<00:00, 308.38it/s]
