# 2023-03-01 11:31

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=metadata/unique
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type          | ref           | access
$DATASET                 |               |               |
  |   |   | Country      |               |               |
  |   |   |   | name     | string unique |               | open
  |   |   | City         |               |               |
  |   |   |   |          | unique        | name, country |
  |   |   |   | name     | string        |               | open
  |   |   |   | country  | string        |               | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
#|                       List of relations
#|  Schema |                Name                | Type  | Owner 
#| --------+------------------------------------+-------+-------
#|  public | metadata/unique/City               | table | admin
#|  public | metadata/unique/City/:changelog    | table | admin
#|  public | metadata/unique/Country            | table | admin
#|  public | metadata/unique/Country/:changelog | table | admin
#| (4 rows)
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#| PRIMARY KEY, btree (_id)
#| UNIQUE CONSTRAINT, btree (name)
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/City"'
#| PRIMARY KEY, btree (_id)
#| UNIQUE CONSTRAINT, btree (name, country)

# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Country" $AUTH name=Lithuania
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "d978db5f-6a80-42a1-9df9-f2577b457832",
#|     "_revision": "fe750c80-ab4d-40a3-9a17-6f9294861025",
#|     "_type": "metadata/unique/Country",
#|     "name": "Lithuania"
#| }

http POST "$SERVER/$DATASET/Country" $AUTH name=Lithuania
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "UniqueConstraint",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "metadata/unique",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "metadata/unique/Country",
#|                 "property": "name",
#|                 "schema": "4"
#|             },
#|             "message": "Given value already exists.",
#|             "template": "Given value already exists.",
#|             "type": "property"
#|         }
#|     ]
#| }

http POST "$SERVER/$DATASET/Country" $AUTH name=Latvia
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "6aa04136-bbe0-40d1-8201-b5fbbf9cad5e",
#|     "_revision": "a1956df8-d7a5-40c3-b4fe-717983940ff5",
#|     "_type": "metadata/unique/Country",
#|     "name": "Latvia"
#| }

http PATCH "$SERVER/$DATASET/Country/6aa04136-bbe0-40d1-8201-b5fbbf9cad5e" $AUTH \
    _revision=a1956df8-d7a5-40c3-b4fe-717983940ff5 \
    name=Lithuania
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "UniqueConstraint",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "metadata/unique",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "metadata/unique/Country",
#|                 "property": "name",
#|                 "schema": "4"
#|             },
#|             "message": "Given value already exists.",
#|             "template": "Given value already exists.",
#|             "type": "property"
#|         }
#|     ]
#| }

http PATCH "$SERVER/$DATASET/Country/6aa04136-bbe0-40d1-8201-b5fbbf9cad5e" $AUTH \
    _revision=a1956df8-d7a5-40c3-b4fe-717983940ff5 \
    name=Poland
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "6aa04136-bbe0-40d1-8201-b5fbbf9cad5e",
#|     "_revision": "81af391d-5653-4675-86c4-eb6626b233e3",
#|     "_type": "metadata/unique/Country",
#|     "name": "Poland"
#| }


http POST "$SERVER/$DATASET/City" $AUTH name=Vilnius country=Lithuania
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "4d147e51-036e-455f-97c0-dea8543bcd2e",
#|     "_revision": "d38f0ce9-2440-4014-aa60-ef036793b6ad",
#|     "_type": "metadata/unique/City",
#|     "country": "Lithuania",
#|     "name": "Vilnius"
#| }

http POST "$SERVER/$DATASET/City" $AUTH name=Vilnius country=Lithuania
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "IntegrityError",
#|             "message": "
#|                 (psycopg2.errors.UniqueViolation)
#|                     duplicate key value violates unique constraint
#|                     metadata/unique/City_name_country_key
#|                 DETAIL:
#|                     Key (name, country)=(Vilnius, Lithuania) already exists.
#|                 SQL:
#|                     INSERT INTO metadata/unique/City (
#|                         _txn,
#|                         _created,
#|                         _id,
#|                         _revision,
#|                         name,
#|                         country
#|                     )
#|                     VALUES (
#|                         %(_txn)s,
#|                         TIMEZONE('utc',
#|                         CURRENT_TIMESTAMP),
#|                         %(_id)s,
#|                         %(_revision)s,
#|                         %(name)s,
#|                         %(country)s
#|                     )
#|                 parameters: {
#|                     '_txn': UUID('f034cb77-ecd9-45f7-a499-2f7ffbb74aed'),
#|                     '_id': UUID('05a5a1a9-5109-4bcb-a259-115ffb92ba32'),
#|                     '_revision': '2e1fb767-c052-4b7c-9bad-3de95ed7690a',
#|                     'name': 'Vilnius',
#|                     'country': 'Lithuania'
#|                 }
#|             "
#|         }
#|     ]
#| }
# FIXME: Should be 400 error.
