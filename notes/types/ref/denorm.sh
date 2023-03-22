# 2023-03-01 11:55

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/ref/denorm
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property     | type    | ref     | access
$DATASET                     |         |         |
  |   |   | Country          |         |         |
  |   |   |   | name         | string  |         | open
  |   |   | City             |         |         |
  |   |   |   | name         | string  |         | open
  |   |   |   | country      | ref     | Country | open
  |   |   |   | country.code |         |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
#| ReferencedPropertyNotFound: Property 'country.code' not found.
#|   Context:
#|     component: spinta.components.Property
#|     manifest: default
#|     schema: 5
#|     dataset: types/ref/denorm
#|     model: types/ref/denorm/City
#|     entity: 
#|     property: country.code
#|     attribute: 
#|     ref: {'property': 'code', 'model': 'types/ref/denorm/Country'}


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property     | type    | ref     | access
$DATASET                     |         |         |
  |   |   | Country          |         |         |
  |   |   |   | name         | string  |         | open
  |   |   |   | code         | string  |         | open
  |   |   | City             |         |         |
  |   |   |   | name         | string  |         | open
  |   |   |   | country      | ref     | Country | open
  |   |   |   | country.name |         |         | open
  |   |   |   | country.code |         |         | open
  |   |   |   | country.year | integer |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show
poetry run spinta check

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|                  Table "public.types/ref/denorm/Country"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _id       | uuid                        |           | not null | 
#|  name      | text                        |           |          | 
#|  code      | text                        |           |          | 
#| Indexes:
#|     "types/ref/denorm/Country_pkey" PRIMARY KEY, btree (_id)

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/City"'
#|                     Table "public.types/ref/denorm/City"
#|     Column    |            Type             | Collation | Nullable | Default 
#| --------------+-----------------------------+-----------+----------+---------
#|  _id          | uuid                        |           | not null | 
#|  name         | text                        |           |          | 
#|  country._id  | uuid                        |           |          | 
#|  country.name | character varying           |           |          | 
#|  country.code | character varying           |           |          | 
# FIXME: Type should be `text` as in Country table.
#|  country.year | integer                     |           |          | 
#| Indexes:
#|     "types/ref/denorm/City_pkey" PRIMARY KEY, btree (_id)
#| Foreign-key constraints:
#|     "fk_types/ref/denorm/City_country._id" FOREIGN KEY ("country._id")
#|         REFERENCES "types/ref/denorm/Country" (_id)

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Country" $AUTH name="Lithuania" code="lt"
#| HTTP/1.1 201 Created
#|
#| {
#|     "_id": "bfb889b8-61fc-43e8-8d32-c06824bee2c3",
#|     "_revision": "c2bf3465-5bab-4bb0-8a52-d09c948f2517",
#|     "_type": "types/ref/denorm/Country",
#|     "code": "lt",
#|     "name": "Lithuania"
#| }

http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "name": "Vilnius",
    "country": {
        "_id": "bfb889b8-61fc-43e8-8d32-c06824bee2c3",
        "name": "Lithuania",
        "code": "lt",
        "year": 1323
    }
}
EOF


http GET "$SERVER/$DATASET/City"
#| {
#|     "_data": [
#|         {
#|             "_id": "d176699e-c589-405c-a4e8-3cdd2c24e54b",
#|             "_revision": "89bb5e9b-7613-433d-b6f2-6003a9cfe59c",
#|             "name": "Vilnius",
#|             "country": {
#|                 "_id": "bfb889b8-61fc-43e8-8d32-c06824bee2c3",
#|                 "name": "Lithuania",
#|                 "code": "lt",
#|                 "year": 1323
#|             }
#|         }
#|     ]
#| }


http GET "$SERVER/$DATASET/City?country.code='lt'&select(name, country.name)&sort(country.name)"
#| {
#|     "_data": [
#|         {
#|             "name": "Vilnius",
#|             "country": {
#|                 "name": "Lithuania"
#|             }
#|         }
#|     ]
#| }
