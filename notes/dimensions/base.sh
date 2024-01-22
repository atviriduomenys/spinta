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

alias query='psql -h localhost -p 54321 -U admin spinta'

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

LTU=2fcc3da3-be88-4715-83c9-a45bcbeeb3c3
VLN=2074d66e-0dfd-4233-b1ec-199abc994d0c

http POST "$SERVER/$DATASET/Place"    $AUTH _id=$LTU id:=1 name=Lithuania
http POST "$SERVER/$DATASET/Location" $AUTH _id=$LTU id:=2 population:=2862380
http POST "$SERVER/$DATASET/Country"  $AUTH _id=$LTU id:=3

http POST "$SERVER/$DATASET/Place" $AUTH \
    _id=$VLN \
    id:=2 \
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
#|                  _id                  | id |   name    |                       koord                        
#| --------------------------------------+----+-----------+----------------------------------------------------
#|  2fcc3da3-be88-4715-83c9-a45bcbeeb3c3 |  1 | Lithuania | 
#|  2074d66e-0dfd-4233-b1ec-199abc994d0c |  2 | Vilnius   | 0101000020E6100000DDEF5014E8574B40A6ED5F59694A3940


http GET "$SERVER/$DATASET/Place?format(ascii)"
#| _id                                   id  name       koord      
#| ------------------------------------  --  ---------  -------------------------
#| 2fcc3da3-be88-4715-83c9-a45bcbeeb3c3  1   Lithuania  ∅
#| 2074d66e-0dfd-4233-b1ec-199abc994d0c  2   Vilnius    POINT (54.68677 25.29067)


http POST "$SERVER/$DATASET/Location" $AUTH \
    _id=$VLN \
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
#|                 "dataset": "dimensions/base",
#|                 "entity": "",
#|                 "feature": "Ability to indirectly modify base parameters",
#|                 "manifest": "default",
#|                 "model": "dimensions/base/Location",
#|                 "property": "name",
#|                 "schema": "10"
#|             },
#|             "message": "Ability to indirectly modify base parameters is not implemented yet.",
#|             "template": "{feature} is not implemented yet.",
#|             "type": "property"
#|         }
#|     ]
#| }
# FIXME: This should not be an error, because we do not modify base, we just use
#        _base.name as identifier, which exists in Place.

query -c 'SELECT * FROM "'$DATASET'/Location"'
#|                  _id                  | id | population | type 
#| --------------------------------------+----+------------+------
#|  2fcc3da3-be88-4715-83c9-a45bcbeeb3c3 |  2 |    2862380 | 

http GET "$SERVER/$DATASET/Location?format(ascii)"
#| _id                                   id  name       population  type
#| ------------------------------------  --  ---------  ----------  ----
#| 2fcc3da3-be88-4715-83c9-a45bcbeeb3c3  2   Lithuania  2862380     ∅


REV=$(http GET "$SERVER/$DATASET/Location/$VLN" | jq -r ._revision)
http PATCH "$SERVER/$DATASET/Location/$VLN" $AUTH _revision=$REV type="city"
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
tail -100 $BASEDIR/spinta.log
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "ReferencedObjectNotFound",
#|             "context": {
#|                 "attribute": null,
#|                 "component": "spinta.components.Property",
#|                 "dataset": "dimensions/base",
#|                 "entity": "",
#|                 "id": "6f0208d9-4e0e-4a57-8c44-9b122b25bd43",
#|                 "manifest": "default",
#|                 "model": "dimensions/base/City",
#|                 "property": "_id",
#|                 "schema": "24"
#|             },
#|             "message": "Referenced object '6f0208d9-4e0e-4a57-8c44-9b122b25bd43' not found.",
#|             "template": "Referenced object {id!r} not found.",
#|             "type": "property"
#|         }
#|     ]
#| }


http POST "$SERVER/$DATASET/City" $AUTH \
    _id=$VLN \
    id:=24 \
    name="Vilnius" \
    "country[_id]=$LTU" \
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

http GET "$SERVER/$DATASET/City?select(id,name,country.name)&format(ascii)"
#| id  name     country.name
#| --  -------  ------------
#| 24  Vilnius  Vilnius
# FIXME: `country.name` should be Lithuania.
