# 2023-04-25 11:40

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/ref/constraints
DATASET=$INSTANCE

# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | level | access
$DATASET                 |         |         |       | 
  |   |   | Country      |         | id      |       | 
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   | City         |         | id      |       | 
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   | country  | ref     | Country | 4     | open
  |   |   |   | twin     | ref     | City    | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

alias query='psql -h localhost -p 54321 -U admin spinta'

query -c '\d "'$DATASET'/City"'
#| FOREIGN KEY  ("country._id")
#|   REFERENCES "types/ref/constraints/Country"(_id)
#| FOREIGN KEY  ("twin._id")
#|   REFERENCES "types/ref/constraints/City"(_id)

# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

uuidgen
#| 9e1ca4d7-d13d-4e48-91f8-627632d7ea28
uuidgen
#| 2beef556-5d48-4034-ac03-9e3bb52f2efe

http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Vilnius",
    "country": {
        "_id": "9e1ca4d7-d13d-4e48-91f8-627632d7ea28"
    },
    "twin": {
        "_id": "2beef556-5d48-4034-ac03-9e3bb52f2efe"
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
#|                 "dataset": "types/ref/constraints",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/ref/constraints/City",
#|                 "property": "twin",
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property 'twin'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         }
#|     ]
#| }

http POST "$SERVER/$DATASET/Country" $AUTH id:=1 name=Lithuania _id=5dd1b4aa-3f2e-420a-a116-f606ac1f7e7d
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "5dd1b4aa-3f2e-420a-a116-f606ac1f7e7d",
#|     "_revision": "6aa459b6-cdfe-48b2-9dff-57aee404bd2e",
#|     "_type": "types/ref/constraints/Country",
#|     "id": 1,
#|     "name": "Lithuania"
#| }

http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Kaunas",
    "country": {
        "_id": "5dd1b4aa-3f2e-420a-a116-f606ac1f7e7d"
    },
    "twin": null
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
#|                 "dataset": "types/ref/constraints",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/ref/constraints/City",
#|                 "property": "twin",
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property 'twin'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         }
#|     ]
#| }
# FIXME: This should not be an error.
