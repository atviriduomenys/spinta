# 2023-02-16 14:27

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=dimensions/base
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type    | ref     | prepare   | access
$DATASET                   |         |         |           |
  |   |   |   |            |         |         |           |     
  |   |   | Location       |         | id      |           |
  |   |   |   | id         | integer |         |           | open
  |   |   |   | name       | string  |         |           | open
  |   |   |   | population | integer |         |           | open
  |   |   |   | type       | string  |         |           | open
  |   |   |   |            | enum    |         | "city"    |     
  |   |   |   |            |         |         | "country" |     
  |   |   |   |            |         |         |           |     
  |   | Location           |         | name    |           |
  |   |   |   |            |         |         |           |     
  |   |   | Country        |         | id      |           |
  |   |   |   | id         | integer |         |           | open
  |   |   |   | name       |         |         |           | open
  |   |   |   | population |         |         |           | open
  |   |   |   |            |         |         |           |     
  |   |   | City           |         | id      |           |
  |   |   |   | id         | integer |         |           | open
  |   |   |   | name       |         |         |           | open
  |   |   |   | population |         |         |           | open
  |   |   |   | country    | ref     | Country |           | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Location" $AUTH \
    _id="2074d66e-0dfd-4233-b1ec-199abc994d0c" \
    id:=42 \
    name="Vilnius" \
    population:=625349
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "8b48b450-782c-4a41-80bb-25d249d6bae5",
#|     "_type": "dimensions/base/Location",
#|     "id": 42,
#|     "name": "Vilnius",
#|     "population": 625349
#|     "type": null
#| }


http PATCH "$SERVER/$DATASET/Location/2074d66e-0dfd-4233-b1ec-199abc994d0c" $AUTH \
    _revision="8b48b450-782c-4a41-80bb-25d249d6bae5" \
    type="city"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "7a75fc7f-6c91-4df7-8dfd-c2af09615f3e",
#|     "_type": "dimensions/base/Location",
#|     "type": "city"
#| }


http POST "$SERVER/$DATASET/City" $AUTH \
    id:=24 \
    name="Vilnius" \
    population:=625349
#| HTTP/1.1 500 Internal Server Error
#| {
#|     "errors": [
#|         {
#|             "code": "IntegrityError",
#|             "message": "
#|                 (psycopg2.errors.ForeignKeyViolation)
#|                 insert or update on table "dimensions/base/City"
#|                 violates foreign key constraint
#|                 "fk_dimensions/base/Location_id"
#|
#|                 DETAIL:
#|                     Key (_id)=(467db78a-e1c4-409d-a0de-4dac885a2d8b)
#|                     is not present in table "dimensions/base/Location".
#|
#|                 SQL:
#|                     INSERT INTO "dimensions/base/City" (
#|                         _txn,
#|                         _created,
#|                         _id,
#|                         _revision,
#|                         id
#|                     )
#|                     VALUES (
#|                         %(_txn)s,
#|                         TIMEZONE('utc', CURRENT_TIMESTAMP),
#|                         %(_id)s,
#|                         %(_revision)s,
#|                         %(id)s
#|                     )
#|
#|                 parameters:
#|                     {
#|                         '_txn': UUID('68a85ce1-50d4-4d05-abc2-bf9ca69cabea'),
#|                         '_id': UUID('467db78a-e1c4-409d-a0de-4dac885a2d8b'),
#|                         '_revision': '40505c9b-0ca5-4ee1-8cae-e81ef2da85c1',
#|                         'id': 24
#|                     }
#|             "
#|         }
#|     ]
#| }
# TODO: This should not be a 500 error.
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "sqlalchemy/engine/base.py", line 1771, in _execute_context
#|     self.dialect.do_execute(
#|   File "sqlalchemy/engine/default.py", line 717, in do_execute
#|     cursor.execute(statement, parameters)
#| psycopg2.errors.ForeignKeyViolation: insert or update on table "dimensions/base/City" violates foreign key constraint "fk_dimensions/base/Location_id"
#| DETAIL:  Key (_id)=(a050ba11-538f-4bd6-9d02-c4717495bfdf) is not present in table "dimensions/base/Location".
#| 
#| The above exception was the direct cause of the following exception:
#| 
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 94, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 201, in create_http_response
#|     return await commands.push(
#|   File "spinta/commands/write.py", line 100, in push
#|     status_code, response = await simple_response(
#|   File "spinta/commands/write.py", line 1122, in simple_response
#|     results = await alist(aslice(results, 2))
#|   File "spinta/utils/aiotools.py", line 51, in alist
#|     return [x async for x in it]
#|   File "spinta/utils/aiotools.py", line 51, in <listcomp>
#|     return [x async for x in it]
#|   File "spinta/utils/aiotools.py", line 62, in aslice
#|     async for x in it:
#|   File "spinta/accesslog/__init__.py", line 174, in log_async_response
#|     async for row in rows:
#|   File "spinta/commands/write.py", line 185, in push_stream
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/changes.py", line 25, in create_changelog_entry
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/write.py", line 33, in insert
#|     connection.execute(qry, patch)
#|   File "sqlalchemy/engine/base.py", line 1263, in execute
#|     return meth(self, multiparams, params, _EMPTY_EXECUTION_OPTS)
#|   File "sqlalchemy/sql/elements.py", line 323, in _execute_on_connection
#|     return connection._execute_clauseelement(
#|   File "sqlalchemy/engine/base.py", line 1452, in _execute_clauseelement
#|     ret = self._execute_context(
#|   File "sqlalchemy/engine/base.py", line 1814, in _execute_context
#|     self._handle_dbapi_exception(
#|   File "sqlalchemy/engine/base.py", line 1995, in _handle_dbapi_exception
#|     util.raise_(
#|   File "sqlalchemy/util/compat.py", line 207, in raise_
#|     raise exception
#|   File "sqlalchemy/engine/base.py", line 1771, in _execute_context
#|     self.dialect.do_execute(
#|   File "sqlalchemy/engine/default.py", line 717, in do_execute
#|     cursor.execute(statement, parameters)
#| sqlalchemy.exc.IntegrityError: (psycopg2.errors.ForeignKeyViolation) insert or update on table "dimensions/base/City" violates foreign key constraint "fk_dimensi
#| ons/base/Location_id"


http POST "$SERVER/$DATASET/City" $AUTH \
    _id="2074d66e-0dfd-4233-b1ec-199abc994d0c" \
    id:=24 \
    name="Vilnius" \
    population:=625349
#| HTTP/1.1 201 Created

http GET "$SERVER/$DATASET/Location?select(_id,id,name,population)&format(ascii)"
#| ------------------------------------  --  -------  ----------
#| _id                                   id  name     population
#| dc918748-0e0c-47ba-9090-c878eabd7fb9  42  Vilnius  625349
#| ------------------------------------  --  -------  ----------

http GET "$SERVER/$DATASET/City?select(_id,id,name,population)&format(ascii)"
#| ------------------------------------  --  -------  ----------
#| _id                                   id  name     population
#| dc918748-0e0c-47ba-9090-c878eabd7fb9  24  Vilnius  625349
#| ------------------------------------  --  -------  ----------
