# 2023-04-24 15:00

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/exitcode
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+
#| | id |  name   |
#| +----+---------+
#| | 1  | Vilnius |
#| +----+---------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref                          | source       | prepare | level | access
$DATASET                  |         |                              |              |         |       |
  | db                    | sql     |                              | sqlite:///$BASEDIR/db.sqlite | | |
  |   |   | City          |         | id                           | cities       |         | 4     |
  |   |   |   | id        | integer |                              | id           |         | 4     | open
  |   |   |   | name      | string  |                              | name         |         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations
# notes/postgres.sh         Show tables

# Run server that responds with error to every request except /auth/token
test -n "$PID" && kill $PID
poetry run pip install bottle
poetry run python &>> $BASEDIR/bottle.log <<'EOF' &
from bottle import route, request, response, run
@route('/auth/token', 'POST')
def token():
    return {'access_token': 'TOKEN'}
@route('<path:path>', method=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'])
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

# Try to push with a 404 response code
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
echo $?
#| 1


test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

rm $BASEDIR/db.sqlite
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| Traceback (most recent call last):t/s]
#|   File "spinta/cli/helpers/data.py", line 80, in _read_model_data
#|     stream = peek(stream)
#|   File "spinta/utils/itertools.py", line 40, in peek
#|     peek = list(islice(it, 1))
#|   File "spinta/datasets/backends/sql/commands/read.py", line 90, in getall
#|     table = backend.get_table(model, table)
#|   File "spinta/datasets/backends/sql/components.py", line 37, in get_table
#|     sa.Table(name, self.schema, autoload_with=self.engine)
#|   <...>
#|   File "sqlalchemy/engine/reflection.py", line 788, in reflect_table
#|     raise exc.NoSuchTableError(table.name)
#| sqlalchemy.exc.NoSuchTableError: cities
echo $?
#| 1
