# 2023-04-26 14:39

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=backends/postgresql/base/write
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


VILNIUS=2074d66e-0dfd-4233-b1ec-199abc994d0c

http POST "$SERVER/$DATASET/Place" $AUTH \
    _id=$VILNIUS \
    id:=1 \
    name="Vilnius" \
    koord="SRID=4326;POINT (54.68677 25.29067)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "ed999a14-6265-42e0-a01f-770fe153891f",
#|     "_type": "backends/postgresql/base/write/Place",
#|     "id": 1,
#|     "koord": "SRID=4326;POINT (54.68677 25.29067)",
#|     "name": "Vilnius"
#| }

query -c 'SELECT * FROM "'$DATASET'/Place"'
#|                  _id                  |              _revision               | id |  name   |                       koord                        
#| --------------------------------------+--------------------------------------+----+---------+----------------------------------------------------
#|  2074d66e-0dfd-4233-b1ec-199abc994d0c | ed999a14-6265-42e0-a01f-770fe153891f |  1 | Vilnius | 0101000020E6100000DDEF5014E8574B40A6ED5F59694A3940


http GET "$SERVER/$DATASET/Place?format(ascii)"
#| _id                                   _revision                             id  name     koord
#| ------------------------------------  ------------------------------------  --  -------  -------------------------
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  ed999a14-6265-42e0-a01f-770fe153891f  1   Vilnius  POINT (54.68677 25.29067)

http POST "$SERVER/$DATASET/Location" $AUTH \
    _id=$VILNIUS \
    id:=42 \
    name="Vilnius" \
    population:=625349
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NotImplementedFeature",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "backends/postgresql/base/write",
#|                 "entity": "",
#|                 "feature": "Ability to indirectly modify base parameters",
#|                 "manifest": "default",
#|                 "model": "backends/postgresql/base/write/Location",
#|                 "property": "name",
#|                 "schema": "10"
#|             },
#|             "message": "Ability to indirectly modify base parameters is not implemented yet.",
#|             "template": "{feature} is not implemented yet.",
#|             "type": "property"
#|         }
#|     ]
#| }
# TODO: https://github.com/atviriduomenys/spinta/issues/262

http POST "$SERVER/$DATASET/Location" $AUTH \
    _id=$VILNIUS \
    id:=42 \
    population:=625349
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "c0a161db-6626-4188-8af8-57e39d999a4c",
#|     "_type": "backends/postgresql/base/write/Location",
#|     "id": 42,
#|     "population": 625349,
#|     "type": "city"
#| }

query -c 'SELECT * FROM "'$DATASET'/Location"'
#|                  _id                  |              _revision               | id | population | type 
#| --------------------------------------+--------------------------------------+----+------------+------
#|  2074d66e-0dfd-4233-b1ec-199abc994d0c | 8303ac22-90f4-4f0a-92a7-08d6b49430fe | 42 |     625349 | 

http GET "$SERVER/$DATASET/Location?format(ascii)"
#| _id                                   _revision                             id  name     population  type
#| ------------------------------------  ------------------------------------  --  -------  ----------  ----
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  8303ac22-90f4-4f0a-92a7-08d6b49430fe  42  Vilnius  625349      ∅


http PATCH "$SERVER/$DATASET/Location/$VILNIUS" $AUTH \
    _revision="c0a161db-6626-4188-8af8-57e39d999a4c" \
    type="city"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "c0a161db-6626-4188-8af8-57e39d999a4c",
#|     "_type": "backends/postgresql/base/write/Location",
#|     "type": "city"
#| }


http PATCH "$SERVER/$DATASET/Location/$VILNIUS" $AUTH \
    _revision="c0a161db-6626-4188-8af8-57e39d999a4c" \
    type="city"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "c0a161db-6626-4188-8af8-57e39d999a4c",
#|     "_type": "backends/postgresql/base/write/Location"
#| }


http GET "$SERVER/$DATASET/Location?select(_id,id,name,population,type)&format(ascii)"
#| _id                                   id  name     population  type
#| ------------------------------------  --  -------  ----------  ----
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  42  Vilnius  625349      city


http POST "$SERVER/$DATASET/City" $AUTH id:=24
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "ReferencedObjectNotFound",
#|             "context": {
#|                 "attribute": null,
#|                 "component": "spinta.components.Property",
#|                 "dataset": "backends/postgresql/base/write",
#|                 "entity": "",
#|                 "id": "306189ae-f07d-4b79-ae1d-78f5f5816bc8",
#|                 "manifest": "default",
#|                 "model": "backends/postgresql/base/write/City",
#|                 "property": "_id",
#|                 "schema": "24"
#|             },
#|             "message": "Referenced object '306189ae-f07d-4b79-ae1d-78f5f5816bc8' not found.",
#|             "template": "Referenced object {id!r} not found.",
#|             "type": "property"
#|         }
#|     ]
#| }


http POST "$SERVER/$DATASET/City" $AUTH _id=$VILNIUS id:=24
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "782826b0-63ce-4949-b24e-e34df3031214",
#|     "_type": "backends/postgresql/base/write/City",
#|     "country": {"_id": null},
#|     "id": 24
#| }


http POST "$SERVER/$DATASET/City" $AUTH _id=$VILNIUS id:=24
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

uuidgen
#| d55e65c6-97c9-4cd3-99ff-ae34e268289b
LITHUANIA=d55e65c6-97c9-4cd3-99ff-ae34e268289b

http POST "$SERVER/$DATASET/Place" $AUTH _id=$LITHUANIA id:=3 \
    name=Lithuania \
    koord="SRID=4326;POINT (54.68677 25.29067)"

http POST "$SERVER/$DATASET/Location" $AUTH _id=$LITHUANIA id:=12 \
    type=country \
    population:=3000000

http POST "$SERVER/$DATASET/Country" $AUTH _id=$LITHUANIA id:=56

REV=$(http GET "$SERVER/$DATASET/City/$VILNIUS" | jq -r ._revision)
http PATCH "$SERVER/$DATASET/City/$VILNIUS" $AUTH _revision=$REV \
    "country[_id]"=$LITHUANIA
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "2074d66e-0dfd-4233-b1ec-199abc994d0c",
#|     "_revision": "6e8e4b5b-8259-452e-8db1-4dd59a233cb1",
#|     "_type": "backends/postgresql/base/write/City",
#|     "country": {
#|         "_id": "d55e65c6-97c9-4cd3-99ff-ae34e268289b"
#|     }
#| }

http GET "$SERVER/$DATASET/Place?format(ascii)"
#| id  name       koord
#| --  ---------  -------------------------
#| 1   Vilnius    POINT (54.68677 25.29067)
#| 3   Lithuania  POINT (54.68677 25.29067)

http GET "$SERVER/$DATASET/Location?format(ascii)"
#| id  name      population  type
#| --  --------  ----------  -------
#| 42  Vilnius   625349      city
#| 12  Lithuani  3000000     country

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| id  name       population
#| --  ---------  ----------
#| 56  Lithuania  3000000

http GET "$SERVER/$DATASET/City?select(id,name,population,country)&format(ascii)"
#| id  name     population  country._id                         
#| --  -------  ----------  ------------------------------------
#| 24  Vilnius  625349      d55e65c6-97c9-4cd3-99ff-ae34e268289b

http GET "$SERVER/$DATASET/City?select(id,name,population,country.name)&format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "KeyError",
#|             "message": "'name'"
#|         }
#|     ]
#| }
# TODO: Fix `county.name` join.
#       https://github.com/atviriduomenys/spinta/issues/437
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 94, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 153, in create_http_response
#|     return await commands.getall(
#|   File "spinta/commands/read.py", line 121, in getall
#|     return render(context, request, model, params, rows, action=action)
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|   File "spinta/formats/ascii/commands.py", line 28, in render
#|     return _render(
#|   File "spinta/formats/ascii/commands.py", line 62, in _render
#|     aiter(peek_and_stream(fmt(
#|   File "spinta/utils/response.py", line 223, in peek_and_stream
#|     peek = list(itertools.islice(stream, 2))
#|   File "spinta/formats/ascii/components.py", line 41, in __call__
#|     peek = next(data, None)
#|   File "spinta/commands/read.py", line 106, in <genexpr>
#|     rows = (
#|   File "spinta/backends/postgresql/commands/read.py", line 51, in getall
#|     expr = env.resolve(query)
#|   File "spinta/backends/postgresql/commands/query.py", line 267, in select
#|     selected = env.call('select', arg)
#|   File "spinta/backends/postgresql/commands/query.py", line 385, in select
#|     column = table.c[fpr.right.place]
#|   File "sqlalchemy/sql/base.py", line 1192, in __getitem__
#|     return self._index[key]
#| KeyError: 'name'
