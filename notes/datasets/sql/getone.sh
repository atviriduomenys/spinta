# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=datasets/sql/getone
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property     | type    | ref | level | source  | access
$DATASET                     |         |     |       |         |
  | db                       | sql     |     |       | sqlite:///$BASEDIR/db.sqlite |
  |   |   | City             |         | id  |       | cities  |
  |   |   |   | id           | integer |     | 4     | id      | open
  |   |   |   | name         | string  |     | 4     | name    | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);

INSERT INTO cities VALUES (1, 'Vilnius');
INSERT INTO cities VALUES (2, 'Kaunas');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _id                                   _revision  _page  id  name   
#| ------------------------------------  ---------  -----  --  -------
#| 47cb18cb-77e7-4216-852d-122c9214a3af  ∅          WzFd   1   Vilnius
#| d8444e1b-912d-470d-bf34-8306637854a5  ∅          WzJd   2   Kaunas

http GET "$SERVER/$DATASET/City/47cb18cb-77e7-4216-852d-122c9214a3af"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NotImplementedError",
#|             "message": "Could not find signature for getone: <Context, Model, Sql>"
#|         }
#|     ]
#| }
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "multipledispatch/dispatcher.py", line 269, in __call__
#|     func = self._cache[types]
#|            ~~~~~~~~~~~^^^^^^^
#| KeyError: (
#|     <class 'spinta.components.Context'>,
#|     <class 'spinta.components.Model'>,
#|     <class 'spinta.datasets.backends.sql.components.Sql'>,
#| )
#| 
#| During handling of the above exception, another exception occurred:
#| 
#| Traceback (most recent call last):
#|   File "gspinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "gspinta/utils/response.py", line 129, in create_http_response
#|     return await commands.getone(
#|            ^^^^^^^^^^^^^^^^^^^^^^
#|   File "gspinta/commands/read.py", line 301, in getone
#|     resp = await commands.getone(
#|            ^^^^^^^^^^^^^^^^^^^^^^
#|   File "gspinta/commands/read.py", line 354, in getone
#|     data = commands.getone(context, model, backend, id_=params.pk)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for getone: <Context, Model, Sql>
