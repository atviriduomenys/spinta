# 2023-03-01 19:44

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=keymap
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id    INTEGER PRIMARY KEY,
    code  TEXT,
    name  TEXT
);
CREATE TABLE IF NOT EXISTS cities (
    name             TEXT PRIMARY KEY,
    country_id       INTEGER,
    country_code     TEXT,
    country_name     TEXT,
    FOREIGN KEY (country_id) REFERENCES countries (id)
);
INSERT INTO countries VALUES (1, 'lt', 'Lithuania');
INSERT INTO cities VALUES ('Vilnius', 1, 'lt', 'Lithuania');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
#| +----+------+-----------+
#| | id | code |   name    |
#| +----+------+-----------+
#| | 1  | lt   | Lithuania |
#| +----+------+-----------+
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +---------+---------+
#| |  name   | country |
#| +---------+---------+
#| | Vilnius | lt      |
#| +---------+---------+


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property     | type    | ref           | source       | access
$DATASET                     |         |               |              |
  | db                       | sql     |               | sqlite:///$BASEDIR/db.sqlite |
  |   |   | Country          |         | id            | countries    |
  |   |   |   | id           | integer |               | id           | open
  |   |   |   | code         | string  |               | code         | open
  |   |   |   | name         | string  |               | name         | open
  |   |   | City             |         | name          | cities       |
  |   |   |   | name         | string  |               | name         | open
  |   |   |   | country      | ref     | Country       | country_id   | open
  |   |   |   | country_code | ref     | Country[code] | country_code | open
  |   |   |   | country_name | ref     | Country[name] | country_name | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show


test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/server.sh    Add client
# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Country?select(_id)&format(ascii)"
#| _id
#| ------------------------------------
#| 0a8198af-a55a-4c98-a783-43b23e60aaff

http GET "$SERVER/$DATASET/City?select(_id,country)&format(ascii)"
#| _id                                   country._id                         
#| ------------------------------------  ------------------------------------
#| 1cf0595b-85e7-4ba3-9a38-87197482d7ae  0a8198af-a55a-4c98-a783-43b23e60aaff

http GET "$SERVER/$DATASET/City?select(_id,country_code)&format(ascii)"
#| _id                                   country_code._id                    
#| ------------------------------------  ------------------------------------
#| 1cf0595b-85e7-4ba3-9a38-87197482d7ae  6a754172-d97e-4527-ac6b-1d87f0951ea2
# FIXME: country_code._id should be 0a8198af-a55a-4c98-a783-43b23e60aaff

http GET "$SERVER/$DATASET/City?select(_id,country_name)&format(ascii)"
#| _id                                   country_name._id                    
#| ------------------------------------  ------------------------------------
#| 1cf0595b-85e7-4ba3-9a38-87197482d7ae  d6a9258a-6f57-41e9-b043-8af7cb3f85a2
# FIXME: country_name._id should be 0a8198af-a55a-4c98-a783-43b23e60aaff

sqlite3 $BASEDIR/keymap.db "select name from sqlite_master where type = 'table' order by name;"
#| +----------------------+
#| |         name         |
#| +----------------------+
#| | keymap/City          |
#| | keymap/Country       |
#| | keymap/Country._code |
#| | keymap/Country._name |
#| | keymap/Country.code  |
#| | keymap/Country.name  |
#| +----------------------+
# FIXME: what are `_code` and `_name`?

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "'$DATASET'/Country";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 0a8198af-a55a-4c98-a783-43b23e60aaff | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "'$DATASET'/Country._code";'
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 6a754172-d97e-4527-ac6b-1d87f0951ea2 | 7b14047ce12579e28dae962a25b010b3ace48367 | �lt    | A26C74     |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "'$DATASET'/Country.code";'
# (empty)

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "'$DATASET'/Country._name";'
#| +--------------------------------------+------------------------------------------+-----------+----------------------+
#| |                 key                  |                   hash                   |   value   |      hex(value)      |
#| +--------------------------------------+------------------------------------------+-----------+----------------------+
#| | d6a9258a-6f57-41e9-b043-8af7cb3f85a2 | e4b44f3d2abb6178f578b558915e5c63f329c2c3 | �Lithuania | A94C69746875616E6961 |
#| +--------------------------------------+------------------------------------------+-----------+----------------------+

sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "'$DATASET'/Country.name";'
# (empty)
