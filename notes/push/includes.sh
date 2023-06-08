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
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": []
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
#| 2023-05-12 13:56:41,105 ERROR: Error when sending and receiving data. Model push/includes/City, data:                                                                     
#|  {
#|     '_id': '1cacb9c9-4350-4df2-8b5b-678c5669d731',
#|     '_op': 'insert',
#|     '_type': 'push/includes/City',
#|     'country': {'_id': '6fbef53b-efa6-4c8c-b13c-37e78197e089'},
# FIXME: This should be: 'country': {'code': '6fbef53b-efa6-4c8c-b13c-37e78197e089'}
#        See: https://github.com/atviriduomenys/spinta/issues/208
#             https://github.com/atviriduomenys/spinta/blob/66d8696ce5a889ceaad9ad9fe4b8def5e6f16ad8/notes/types/ref/external.sh#L111
#|     'id': 1,
#|     'name': 'Vilnius',
#| }
#| Server response (status=400):
#|     {
#|         'errors': [
#|             {
#|                 'code': 'FieldNotInResource',
#|                 'context': {
#|                     'attribute': 'country',
#|                     'component': 'spinta.components.Property',
#|                     'dataset': 'push/includes',
#|                     'entity': 'cities',
#|                     'id': '1cacb9c9-4350-4df2-8b5b-678c5669d731',
#|                     'manifest': 'default',
#|                     'model': 'push/includes/City',
#|                     'property': '_id',
#|                     'resource': 'db',
#|                     'resource.backend': 'push/includes/db',
#|                     'schema': '5',
#|                 },
#|                 'message': "Unknown property '_id'.",
#|                 'template': 'Unknown property {property!r}.',
#|                 'type': 'property',
#|             },
#|         ],
#|     }
ERROR: Error on _get_row_count({model.name}).
#| Traceback (most recent call last):
#|   File "multipledispatch/dispatcher.py", line 269, in __call__
#|     func = self._cache[types]
#| KeyError: (<class 'spinta.backends.postgresql.commands.query.PgQueryBuilder'>, <class 'NoneType'>)
#| 
#| During handling of the above exception, another exception occurred:
#| 
#| Traceback (most recent call last):
#|   File "spinta/core/ufuncs.py", line 216, in call
#|     return ufunc(self, *args, **kwargs)
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for select: <PgQueryBuilder, NoneType>
#| 
#| During handling of the above exception, another exception occurred:
#| 
#| Traceback (most recent call last):
#|   File "spinta/cli/helpers/data.py", line 51, in count_rows
#|     count = _get_row_count(context, model)
#|   File "spinta/cli/helpers/data.py", line 36, in _get_row_count
#|     for data in stream:
#|   File "spinta/backends/postgresql/commands/read.py", line 51, in getall
#|     expr = env.resolve(query)
#|   File "spinta/core/ufuncs.py", line 242, in resolve
#|     return ufunc(self, expr)
#|   File "multipledispatch/dispatcher.py", line 278, in __call__
#|     return func(*args, **kwargs)
#|   File "spinta/backends/postgresql/commands/query.py", line 285, in select
#|     selected = env.call('select', arg)
#|   File "spinta/core/ufuncs.py", line 218, in call
#|     raise UnknownMethod(expr=str(Expr(name, *args, **kwargs)), name=name)
#| spinta.exceptions.UnknownMethod: Unknown method 'select' with args select(null).
#|   Context:
#|     expr: select(null)
#|     name: select
# FIXME:
