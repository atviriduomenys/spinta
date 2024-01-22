# 2023-03-01 11:55

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/denorm
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

# Check if error is show for an unknown denormalized property.
cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property            | type    | ref     | access
$DATASET                            |         |         |
  |   |   | Country                 |         |         |
  |   |   |   | code                | string  |         | open
  |   |   |   | name                | string  |         | open
  |   |   | City                    |         |         |
  |   |   |   | name                | string  |         | open
  |   |   |   | country             | ref     | Country | open
  |   |   |   | country.year        |         |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
#| ReferencedPropertyNotFound: Property 'country.year' not found.
#|   Context:
#|     component: spinta.components.Property
#|     manifest: default
#|     schema: 3
#|     dataset: types/denorm
#|     model: types/denorm/Country
#|     entity: 
#|     property: country.year
#|     attribute: 
#|     ref: {'property': 'year', 'model': 'types/denorm/Country'}
# FIXME: Error message is a bit misleading, because it tells, that error in on
#        Country model, but actually error is on City model and country.year
#        denormalized property of City model.


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property            | type    | ref     | access
$DATASET                            |         |         |
  |   |   | Planet                  |         |         |
  |   |   |   | code                | string  |         | open
  |   |   |   | name                | string  |         | open
  |   |   | Country                 |         |         |
  |   |   |   | code                | string  |         | open
  |   |   |   | name                | string  |         | open
  |   |   |   | planet              | ref     | Planet  | open
  |   |   |   | planet.code         |         |         | open
  |   |   | City                    |         |         |
  |   |   |   | name                | string  |         | open
  |   |   |   | country             | ref     | Country | open
  |   |   |   | country.code        |         |         | open
  |   |   |   | country.name        | string  |         | open
  |   |   |   | country.year        | integer |         | open
  |   |   |   | country.planet.code |         |         | open
  |   |   |   | country.planet.name | string  |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show
poetry run spinta check

# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
#|                     List of relations
#|  Schema |              Name               | Type  | Owner 
#| --------+---------------------------------+-------+-------
#|  public | types/denorm/City               | table | admin
#|  public | types/denorm/City/:changelog    | table | admin
#|  public | types/denorm/Country            | table | admin
#|  public | types/denorm/Country/:changelog | table | admin
#|  public | types/denorm/Planet             | table | admin
#|  public | types/denorm/Planet/:changelog  | table | admin

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Planet"'
#|                     Table "public.types/denorm/Planet"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _txn      | uuid                        |           |          | 
#|  _created  | timestamp without time zone |           |          | 
#|  _updated  | timestamp without time zone |           |          | 
#|  _id       | uuid                        |           | not null | 
#|  _revision | text                        |           |          | 
#|  code      | text                        |           |          | 
#|  name      | text                        |           |          | 
#| Indexes:
#|     "types/denorm/Planet_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/denorm/Planet__txn" btree (_txn)
#| Referenced by:
#|     TABLE "types/denorm/Country"
#|         CONSTRAINT "fk_types/denorm/Country_planet._id"
#|             FOREIGN KEY ("planet._id")
#|                 REFERENCES "types/denorm/Planet"(_id)

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|                     Table "public.types/denorm/Country"
#|    Column   |            Type             | Collation | Nullable | Default 
#| ------------+-----------------------------+-----------+----------+---------
#|  _txn       | uuid                        |           |          | 
#|  _created   | timestamp without time zone |           |          | 
#|  _updated   | timestamp without time zone |           |          | 
#|  _id        | uuid                        |           | not null | 
#|  _revision  | text                        |           |          | 
#|  code       | text                        |           |          | 
#|  name       | text                        |           |          | 
#|  planet._id | uuid                        |           |          | 
#| Indexes:
#|     "types/denorm/Country_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/denorm/Country__txn" btree (_txn)
#| Foreign-key constraints:
#|     "fk_types/denorm/Country_planet._id"
#|         FOREIGN KEY ("planet._id")
#|             REFERENCES "types/denorm/Planet"(_id)
#| Referenced by:
#|     TABLE "types/denorm/City"
#|         CONSTRAINT "fk_types/denorm/City_country._id"
#|             FOREIGN KEY ("country._id")
#|                 REFERENCES "types/denorm/Country"(_id)

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/City"'
#|                       Table "public.types/denorm/City"
#|    Column    |            Type             | Collation | Nullable | Default 
#| -------------+-----------------------------+-----------+----------+---------
#|  _txn        | uuid                        |           |          | 
#|  _created    | timestamp without time zone |           |          | 
#|  _updated    | timestamp without time zone |           |          | 
#|  _id         | uuid                        |           | not null | 
#|  _revision   | text                        |           |          | 
#|  name        | text                        |           |          | 
#|  country._id | uuid                        |           |          | 
# FIXME: Own properties (when property type is set) should be stored on the same
#       model. Following properties should be created in this table:
#       - country.name: string
#       - country.year: integer
#       - country.planet.name: string
#| Indexes:
#|     "types/denorm/City_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/denorm/City__txn" btree (_txn)
#| Foreign-key constraints:
#|     "fk_types/denorm/City_country._id"
#|         FOREIGN KEY ("country._id")
#|             REFERENCES "types/denorm/Country"(_id)


# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

ERT=a6ea64a9-47ab-459c-9bf7-e5302f60ce2d
http POST "$SERVER/$DATASET/Planet" $AUTH _id=$ERT code="er" name="Earth"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "a6ea64a9-47ab-459c-9bf7-e5302f60ce2d",
#|     "_revision": "ee781210-7423-4c4e-8213-1981eab13f3d",
#|     "_type": "types/denorm/Planet",
#|     "code": "er",
#|     "name": "Earth"
#| }

LTU=bfb889b8-61fc-43e8-8d32-c06824bee2c3
http POST "$SERVER/$DATASET/Country" $AUTH <<EOF
{
    "_id": "$LTU",
    "code": "lt",
    "name": "Lithuania",
    "planet": {"_id": "$ERT"}
}
EOF
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "bfb889b8-61fc-43e8-8d32-c06824bee2c3",
#|     "_revision": "ae2d5ec7-a6cb-4efd-a74f-e4ec86f59af2",
#|     "_type": "types/denorm/Country",
#|     "code": "lt",
#|     "name": "Lithuania",
#|     "planet": {
#|         "_id": "a6ea64a9-47ab-459c-9bf7-e5302f60ce2d"
#|     }
#| }


VLN=76d25447-92ca-4c07-b396-9e1344abb1a3
http POST "$SERVER/$DATASET/City" $AUTH <<EOF
{
    "_id": "$VLN",
    "name": "Vilnius",
    "country": {
        "_id": "$LTU",
        "name": "Lietuva",
        "year": 1323,
        "planet": {
            "name": "Žemė"
        }
    }
}
EOF
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "component": "spinta.components.Model",
#|                 "dataset": "types/denorm",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/denorm/Country",
#|                 "property": "year",
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property 'year'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         }
#|     ]
#| }
# FIXME: That should not be an error, because `City.country.year` is City's own
#        property and might or might not exist on Country.
