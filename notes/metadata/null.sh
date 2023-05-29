# 2023-03-01 10:12

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=metadata/null
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type                     | access
$DATASET                 |                          | 
  |   |   | City         |                          | 
  |   |   |   | name     | string required          | open
  |   |   |   | country  | string                   | open
  |   |   |   | koords   | geometry(point) required | open
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

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/City"'
#|                     Table "public.metadata/null/City"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  name      | text                        |           | not null | 
#|  country   | text                        |           |          | 
#|  koords    | geometry(Point)             |           | not null | 

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/City" $AUTH \
    name="Vilnius" \
    country="lt" \
    koords="POINT (54.687222 25.28)"
#| HTTP/1.1 201 Created

http POST "$SERVER/$DATASET/City" $AUTH country="lt"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "RequiredProperty",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "metadata/null",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "metadata/null/City",
#|                 "property": "name",
#|                 "schema": "4"
#|             },
#|             "message": "Property is required.",
#|             "template": "Property is required.",
#|             "type": "property"
#|         }
#|     ]
#| }
# TODO: multiple errors should be shown, in this case koords is also a required
#       property, but is not shown in the message
#       https://github.com/atviriduomenys/spinta/issues/378


http GET "$SERVER/$DATASET/City"
#| {
#|     "_data": [
#|         {
#|             "_id": "d176699e-c589-405c-a4e8-3cdd2c24e54b",
#|             "_revision": "89bb5e9b-7613-433d-b6f2-6003a9cfe59c",
#|             "_type": "metadata/null/City",
#|             "country": "lt",
#|             "koords": "POINT (54.687222 25.28)",
#|             "name": "Vilnius"
#|         }
#|     ]
#| }


http PATCH "$SERVER/$DATASET/City/d176699e-c589-405c-a4e8-3cdd2c24e54b" $AUTH \
    _revision=89bb5e9b-7613-433d-b6f2-6003a9cfe59c \
    name:=null
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "RequiredProperty",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "metadata/null",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "metadata/null/City",
#|                 "property": "name",
#|                 "schema": "4"
#|             },
#|             "message": "Property is required.",
#|             "template": "Property is required.",
#|             "type": "property"
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/City?format(ascii)" 
#| _type               _id                                   _revision                             name     country  koords        
#| ------------------  ------------------------------------  ------------------------------------  -------  -------  --------------
#| metadata/null/City  0e1648d9-ec64-4edc-bd27-33e0f7a337d3  1a3aafbd-3079-4f1b-87e5-767272703696  Vilnius  lt       POINT (54.6872\
#|                                                                                                                   22 25.28)

http PATCH "$SERVER/$DATASET/City/0e1648d9-ec64-4edc-bd27-33e0f7a337d3" $AUTH \
    _revision=1a3aafbd-3079-4f1b-87e5-767272703696 \
    name=Kaunas
#| HTTP/1.1 200 OK
#|
#| {
#|     "_id": "0e1648d9-ec64-4edc-bd27-33e0f7a337d3",
#|     "_revision": "c17344fa-aa89-4d70-bc5f-3a433d85eeee",
#|     "_type": "metadata/null/City",
#|     "name": "Kaunas"
#| }

http GET "$SERVER/$DATASET/City"
#| {
#|     "_data": [
#|         {
#|             "_id": "0e1648d9-ec64-4edc-bd27-33e0f7a337d3",
#|             "_revision": "c17344fa-aa89-4d70-bc5f-3a433d85eeee",
#|             "_type": "metadata/null/City",
#|             "country": "lt",
#|             "koords": "POINT (54.687222 25.28)",
#|             "name": "Kaunas"
#|         }
#|     ]
#| }
