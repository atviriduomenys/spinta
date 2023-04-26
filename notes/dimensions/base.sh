# 2023-02-16 14:27

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=dimensions/base
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type                 | ref     | prepare   | access
$DATASET                   |                      |         |           |
  |   |   |   |            |                      |         |           |     
  |   |   | Place          |                      | id      |           |
  |   |   |   | id         | integer              |         |           | open
  |   |   |   | name       | string               |         |           | open
  |   |   |   | koord      | geometry(point,4326) |         |           | open
  |   |   |   |            |                      |         |           |     
  |   | Place              |                      | name    |           |
  |   |   |   |            |                      |         |           |     
  |   |   | Location       |                      | id      |           |
  |   |   |   | id         | integer              |         |           | open
  |   |   |   | name       |                      |         |           | open
  |   |   |   | population | integer              |         |           | open
  |   |   |   | type       | string               |         |           | open
  |   |   |   |            | enum                 |         | "city"    |     
  |   |   |   |            |                      |         | "country" |     
  |   |   |   |            |                      |         |           |     
  |   | Location           |                      | name    |           |
  |   |   |   |            |                      |         |           |     
  |   |   | Country        |                      | id      |           |
  |   |   |   | id         | integer              |         |           | open
  |   |   |   | name       |                      |         |           | open
  |   |   |   | population |                      |         |           | open
  |   |   |   |            |                      |         |           |     
  |   |   | City           |                      | id      |           |
  |   |   |   | id         | integer              |         |           | open
  |   |   |   | name       |                      |         |           | open
  |   |   |   | population |                      |         |           | open
  |   |   |   | country    | ref                  | Country |           | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

query -c '\dt "'$DATASET'"*'
#|  Schema |                Name                 | Type  | Owner 
#| --------+-------------------------------------+-------+-------
#|  public | dimensions/base/City                | table | admin
#|  public | dimensions/base/City/:changelog     | table | admin
#|  public | dimensions/base/Country             | table | admin
#|  public | dimensions/base/Country/:changelog  | table | admin
#|  public | dimensions/base/Location            | table | admin
#|  public | dimensions/base/Location/:changelog | table | admin
#|  public | dimensions/base/Place               | table | admin
#|  public | dimensions/base/Place/:changelog    | table | admin
query -c '\d "'$DATASET'/Place"'
#|                    Table "public.dimensions/base/Place"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _id       | uuid                        |           | not null | 
#|  _revision | text                        |           |          | 
#|  id        | integer                     |           |          | 
#|  name      | text                        |           |          | 
#|  koord     | geometry(Point,4326)        |           |          | 
#| Indexes:
#|     "dimensions/base/Place_pkey" PRIMARY KEY, btree (_id)
#|     "idx_dimensions/base/Place_koord" gist (koord)
#| Referenced by:
#|     TABLE ""dimensions/base/Location""
#|         CONSTRAINT "fk_dimensions/base/Place_id" FOREIGN KEY (_id)
#|             REFERENCES "dimensions/base/Place"(_id)
query -c '\d "'$DATASET'/Location"'
#|                   Table "public.dimensions/base/Location"
#|    Column   |            Type             | Collation | Nullable | Default 
#| ------------+-----------------------------+-----------+----------+---------
#|  _id        | uuid                        |           | not null | 
#|  _revision  | text                        |           |          | 
#|  id         | integer                     |           |          | 
#|  population | integer                     |           |          | 
#|  type       | text                        |           |          | 
#| Indexes:
#|     "dimensions/base/Location_pkey" PRIMARY KEY, btree (_id)
#| Foreign-key constraints:
#|     "fk_dimensions/base/Place_id" FOREIGN KEY (_id)
#|         REFERENCES "dimensions/base/Place"(_id)
#| Referenced by:
#|     TABLE ""dimensions/base/Country""
#|         CONSTRAINT "fk_dimensions/base/Location_id" FOREIGN KEY (_id)
#|             REFERENCES "dimensions/base/Location"(_id)
#|     TABLE ""dimensions/base/City""
#|         CONSTRAINT "fk_dimensions/base/Location_id" FOREIGN KEY (_id)
#|             REFERENCES "dimensions/base/Location"(_id)
query -c '\d "'$DATASET'/Country"'
#|                   Table "public.dimensions/base/Country"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _id       | uuid                        |           | not null | 
#|  _revision | text                        |           |          | 
#|  id        | integer                     |           |          | 
#| Indexes:
#|     "dimensions/base/Country_pkey" PRIMARY KEY, btree (_id)
#| Foreign-key constraints:
#|     "fk_dimensions/base/Location_id" FOREIGN KEY (_id)
#|         REFERENCES "dimensions/base/Location"(_id)
#| Referenced by:
#|     TABLE ""dimensions/base/City""
#|         CONSTRAINT "fk_dimensions/base/City_country._id" FOREIGN KEY ("country._id")
#|             REFERENCES "dimensions/base/Country"(_id)
query -c '\d "'$DATASET'/City"'
#|                     Table "public.dimensions/base/City"
#|    Column    |            Type             | Collation | Nullable | Default 
#| -------------+-----------------------------+-----------+----------+---------
#|  _id         | uuid                        |           | not null | 
#|  _revision   | text                        |           |          | 
#|  id          | integer                     |           |          | 
#|  country._id | uuid                        |           |          | 
#| Indexes:
#|     "dimensions/base/City_pkey" PRIMARY KEY, btree (_id)
#| Foreign-key constraints:
#|     "fk_dimensions/base/City_country._id" FOREIGN KEY ("country._id")
#|         REFERENCES "dimensions/base/Country"(_id)
#|     "fk_dimensions/base/Location_id" FOREIGN KEY (_id)
#|         REFERENCES "dimensions/base/Location"(_id)


http POST "$SERVER/$DATASET/Place" $AUTH \
    _id="2074d66e-0dfd-4233-b1ec-199abc994d0c" \
    id:=1 \
    name="Vilnius" \
    koord="SRID=4326;POINT (54.68677 25.29067)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "2263e065-e813-4ef2-b43c-9699fc4ed212",
#|     "_type": "dimensions/base/Place",
#|     "id": 1,
#|     "koord": "SRID=4326;POINT (54.68677 25.29067)",
#|     "name": "Vilnius"
#| }

query -c 'SELECT * FROM "'$DATASET'/Place"'
#|                  _id                  |              _revision               | id |  name   |                       koord                        
#| --------------------------------------+--------------------------------------+----+---------+----------------------------------------------------
#|  2074d66e-0dfd-4233-b1ec-199abc994d0c | 2263e065-e813-4ef2-b43c-9699fc4ed212 |  1 | Vilnius | 0101000020E6100000DDEF5014E8574B40A6ED5F59694A3940


http GET "$SERVER/$DATASET/Place?format(ascii)"
#| _id                                   _revision                             id  name     koord
#| ------------------------------------  ------------------------------------  --  -------  -------------------------
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  2263e065-e813-4ef2-b43c-9699fc4ed212  1   Vilnius  POINT (54.68677 25.29067)


http POST "$SERVER/$DATASET/Location" $AUTH \
    _id="2074d66e-0dfd-4233-b1ec-199abc994d0c" \
    id:=42 \
    name="Vilnius" \
    population:=625349
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "dcd6e164-5189-4291-b61c-b84d5c1b576c",
#|     "_type": "dimensions/base/Location",
#|     "id": 42,
#|     "name": "Vilnius",
#|     "population": 625349,
#|     "type": null
#| }

query -c 'SELECT * FROM "'$DATASET'/Location"'
#|                  _id                  |              _revision               | id | population | type 
#| --------------------------------------+--------------------------------------+----+------------+------
#|  2074d66e-0dfd-4233-b1ec-199abc994d0c | 8303ac22-90f4-4f0a-92a7-08d6b49430fe | 42 |     625349 | 

http GET "$SERVER/$DATASET/Location?format(ascii)"
#| _id                                   _revision                             id  name     population  type
#| ------------------------------------  ------------------------------------  --  -------  ----------  ----
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  8303ac22-90f4-4f0a-92a7-08d6b49430fe  42  Vilnius  625349      ∅


http PATCH "$SERVER/$DATASET/Location/2074d66e-0dfd-4233-b1ec-199abc994d0c" $AUTH \
    _revision="dcd6e164-5189-4291-b61c-b84d5c1b576c" \
    type="city"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "12274579-32cc-448d-962d-7d0fc82de8d8",
#|     "_type": "dimensions/base/Location",
#|     "type": "city"
#| }


http GET "$SERVER/$DATASET/Location?select(_id,id,name,population,type)&format(ascii)"
#| _id                                   id  name     population  type
#| ------------------------------------  --  -------  ----------  ----
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  42  Vilnius  625349      city


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
#       https://github.com/atviriduomenys/spinta/issues/205
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


http POST "$SERVER/$DATASET/City" $AUTH \
    _id="2074d66e-0dfd-4233-b1ec-199abc994d0c" \
    id:=24 \
    name="Vilnius" \
    population:=625349
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "UniqueConstraint",
#|             "context": {
#|                 "attribute": null,
#|                 "component": "spinta.components.Property",
#|                 "dataset": "dimensions/base",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "dimensions/base/City",
#|                 "property": "_id",
#|                 "schema": "18"
#|             },
#|             "message": "Given value already exists.",
#|             "template": "Given value already exists.",
#|             "type": "property"
#|         }
#|     ]
#| }


http GET "$SERVER/$DATASET/Location?select(_id,id,name,population,type)&format(ascii)"
#| _id                                   id  name     population  type
#| ------------------------------------  --  -------  ----------  ----
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  42  Vilnius  625349      city


http GET "$SERVER/$DATASET/City?select(_id,id,name,population,type,koord)&format(ascii)"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "component": "spinta.components.Model",
#|                 "dataset": "dimensions/base",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "dimensions/base/City",
#|                 "property": "koord",
#|                 "schema": "24"
#|             },
#|             "message": "Unknown property 'koord'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         },
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "component": "spinta.components.Model",
#|                 "dataset": "dimensions/base",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "dimensions/base/City",
#|                 "property": "type",
#|                 "schema": "24"
#|             },
#|             "message": "Unknown property 'type'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         }
#|     ]
#| }
# TODO: There should be no error here, `type` is defined in base (Location),
#       `koord` is defined in `Place` and should be retrieved from there via
#       join.
#
#       https://github.com/atviriduomenys/spinta/issues/395
