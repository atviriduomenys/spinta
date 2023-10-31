# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/sync/private
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    country      TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius', 'lt');
INSERT INTO cities VALUES (2, 'Ryga', 'lv');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+---------+
#| | id |  name   | country |
#| +----+---------+---------+
#| | 1  | Vilnius | lt      |
#| | 2  | Ryga    | lv      |
#| +----+---------+---------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref                              | source                       | level | access
$DATASET/external        |         |                                  |                              |       |
  |   |   | Country      |         | id                               |                              | 4     |
  |   |   |   | id       | integer |                                  |                              | 4     | private
  |   |   |   | code     | string  |                                  |                              | 4     | private
  |   |   |   | name     | string  |                                  |                              | 4     | private
$DATASET                 |         |                                  |                              |       |
  | db                   | sql     |                                  | sqlite:///$BASEDIR/db.sqlite |       |
  |   |   | City         |         | id                               | cities                       | 4     |
  |   |   |   | id       | integer |                                  | id                           | 4     | open
  |   |   |   | name     | string  |                                  | name                         | 4     | open
  |   |   |   | country  | ref     | /$DATASET/external/Country[code] | country                      | 4     | private
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

test -f $BASEDIR/keymap.db && rm $BASEDIR/keymap.db
test -f $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db
poetry run spinta --tb=native push $BASEDIR/manifest.csv -o test@localhost

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/external/Country?format(ascii)"
#| HTTP/1.1 401 Unauthorized

http GET "$SERVER/$DATASET/external/Country?format(ascii)" $AUTH
#| HTTP/1.1 403 Unauthorized
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientScopeError",
#|             "message": "insufficient_scope: Missing one of scopes: spinta_push_sync_private_external_country_getall"
#|         }
#|     ]
#| }
tail -50 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 166, in create_http_response
#|     return await commands.getall(
#|   File "spinta/commands/read.py", line 46, in getall
#|     commands.authorize(context, action, model)
#|   File "spinta/types/model.py", line 472, in authorize
#|     authorized(context, model, action, throw=True)
#|   File "spinta/auth.py", line 476, in authorized
#|     token.check_scope(scopes, operator='OR')
#|   File "spinta/auth.py", line 190, in check_scope
#|     raise InsufficientScopeError(description=f"Missing one of scopes: {missing_scopes}")
#| authlib.oauth2.rfc6750.errors.InsufficientScopeError: insufficient_scope:
#| Missing one of scopes:
#| - spinta_push_sync_private_external_country_getall
# TODO: there should be list of scopes:
#       - spinta_push_sync_private_external_country_getall
#       - spinta_push_sync_private_external_getall
#       - spinta_push_sync_private_getall
#       - spinta_push_sync_getall
#       - spinta_push_getall
#       - spinta_getall
#       But only one scope is considered.
#| INFO:     127.0.0.1:44610 - "GET /push/sync/private/external/Country?format(ascii) HTTP/1.1" 403 Forbidden


http GET "$SERVER/$DATASET/City?format(ascii)"
#| --  -------
#| id  name   
#| 1   Vilnius
#| 2   Ryga
