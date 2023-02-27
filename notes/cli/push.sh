# 2023-02-21 14:23

git log -1 --oneline
#| fe9155c (HEAD -> 311-push-does-not-repeat-errored-chunks, origin/311-push-does-not-repeat-errored-chunks) 311 add repeat count to push command

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
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &
PID=$!
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
poetry run spinta run &>> $BASEDIR/spinta.log &
PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

rm $BASEDIR/push/localhost.db
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 2/2 [00:00<00:00, 21.41it/s]

http GET "$SERVER/$DATASET/Country?format(ascii)"
#|      _type                         _id                                 _revision                 id     name   
#| ===============================================================================================================
#| cli/push/Country   f2c62384-1591-416c-8ed6-40a89c682e0f   cdcbe598-cacb-41a8-acc1-f4955f5a8b70   1    Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
#|     _type                       _id                                 _revision                 id    name                 country._id             
#| =================================================================================================================================================
#| cli/push/City   820f9c83-8654-4a72-9c71-7958a492b41f   5ab09c39-80ed-4cf3-97e8-a59631271dcf   1    Vilnius   f2c62384-1591-416c-8ed6-40a89c682e0f

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
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 197a04e0-4a5c-4598-b365-d9feafedd633 | 2023-02-23 15:11:14.030026 | 0       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 38666f55-bcf8-4c62-bf29-c9d5f081cc82 | 2023-02-23 15:11:14.029174 | 0       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+

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
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 100347f8-2ff0-4add-b75c-9936d56075d1 | 2023-02-23 15:17:45.345100 | 1       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 38666f55-bcf8-4c62-bf29-c9d5f081cc82 | 2023-02-23 15:17:45.330112 | 0       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+

http GET "$SERVER/$DATASET/Country?format(ascii)"
#|      _type                         _id                                 _revision                 id     name   
#| ===============================================================================================================
#| cli/push/Country   f2c62384-1591-416c-8ed6-40a89c682e0f   cdcbe598-cacb-41a8-acc1-f4955f5a8b70   1    Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
# Vilnius was deleted, indeed.

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#|  _op              _created                            _id                     name  
#| ====================================================================================
#| delete   2023-02-23T13:36:00.213711   820f9c83-8654-4a72-9c71-7958a492b41f   None   
#| insert   2023-02-23T13:32:59.423251   820f9c83-8654-4a72-9c71-7958a492b41f   Vilnius

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
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 100347f8-2ff0-4add-b75c-9936d56075d1 | 2023-02-23 15:17:45.345100 | 1       | 0     |
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 6bf5ebfc-f05f-4f30-8a40-1fa1b7ca0a21 | 2023-02-23 15:20:12.495686 | 0       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | f2c62384-1591-416c-8ed6-40a89c682e0f | cdf4be3a371852d382fdf88a6fdfcba1c75e9c29 | 38666f55-bcf8-4c62-bf29-c9d5f081cc82 | 2023-02-23 15:20:12.479892 | 0       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+

http GET "$SERVER/$DATASET/Country?format(ascii)"
#|      _type                         _id                                 _revision                 id     name   
#| ===============================================================================================================
#| cli/push/Country   f2c62384-1591-416c-8ed6-40a89c682e0f   cdcbe598-cacb-41a8-acc1-f4955f5a8b70   1    Lithuania
http GET "$SERVER/$DATASET/City?format(ascii)"
#|     _type                       _id                                 _revision                 id    name                country._id             
#| ================================================================================================================================================
#| cli/push/City   2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd   2349c1c0-e612-4151-b7c5-a9ee8b8f137d   2    Kaunas   f2c62384-1591-416c-8ed6-40a89c682e0f

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
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 61299eff-cf8a-47bf-a94b-b00c2073b990 | 2023-02-23 16:02:34.949586 | 1       | 0     |
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2349c1c0-e612-4151-b7c5-a9ee8b8f137d | 2023-02-23 16:02:34.950118 | 0       | 0     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
# TODO: Vilnius is still marked as deleted, even if this object was reintroduced.

http GET "$SERVER/$DATASET/City?format(ascii)"
#|     _type                       _id                                 _revision                 id    name                country._id             
#| ================================================================================================================================================
#| cli/push/City   2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd   2349c1c0-e612-4151-b7c5-a9ee8b8f137d   2    Kaunas   f2c62384-1591-416c-8ed6-40a89c682e0f
# TODO: Yes, Vilnius should be added back.

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#|  _op              _created                            _id                     name  
#| ====================================================================================
#| insert   2023-02-23T13:59:13.545899   2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd   Kaunas 
#| delete   2023-02-23T13:36:00.213711   820f9c83-8654-4a72-9c71-7958a492b41f   None   
#| insert   2023-02-23T13:32:59.423251   820f9c83-8654-4a72-9c71-7958a492b41f   Vilnius
# TODO: Second insert operation should appear in changelog


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

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | 61299eff-cf8a-47bf-a94b-b00c2073b990 | 2023-02-24 08:24:49.097943 | 1       | 0     |
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 2349c1c0-e612-4151-b7c5-a9ee8b8f137d | 2023-02-24 08:24:49.098478 | 0       | 0     |
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b |                                      | 2023-02-24 08:24:49.130477 | 0       | 1     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+

# Bring a functioning server back online
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &
PID=$!
tail -50 $BASEDIR/spinta.log

# Try to push and check if errored row is retried
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | deleted | error |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
#| | 820f9c83-8654-4a72-9c71-7958a492b41f | add39bc13c5ce30b6c7bc08ae2fed66e112552e3 | bec7318a-bf3d-4cea-8ed6-98e6c90580ee | 2023-02-27 14:27:35.148487 | 1       | 0     |
#| | 2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd | b7c794dde2891313b297207300e951d2056c4463 | 52c73040-3d14-4ab4-83e2-df66ad714b7f | 2023-02-27 14:27:35.149036 | 0       | 0     |
#| | c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971 | 3d2d16d530de2a80018ef76248d8cb6c6481f50b |                                      | 2023-02-27 14:27:35.182293 | 0       | 1     |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+---------+-------+
# FIXME: It seems, that nothing has changed.

tail -50 $BASEDIR/spinta.log
#| spinta.exceptions.ItemDoesNotExist: Resource 'c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971' not found.
#|   Context:
#|     component: spinta.components.Model
#|     manifest: default
#|     schema: 9
#|     dataset: cli/push
#|     resource: db
#|     model: cli/push/City
#|     entity: cities
#|     resource.backend: cli/push/db
#|     id: c3dc2dad-944a-4eb7-bdad-2a0ddf4fb971

http GET "$SERVER/$DATASET/City?format(ascii)"
#|     _type                       _id                                 _revision                 id    name                country._id             
#| ================================================================================================================================================
#| cli/push/City   2a6da1e8-45f9-4b07-ac3c-91b48a90bdcd   52c73040-3d14-4ab4-83e2-df66ad714b7f   2    Kaunas   f2c62384-1591-416c-8ed6-40a89c682e0f
# FIXME: Klaipėda should be in the list of cieties, but is is not.
