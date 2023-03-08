# 2023-02-16 14:27

git log -1 --oneline
#| c8bed7f (HEAD -> 325-add-support-for-base-metadata-in-dsa, origin/325-add-support-for-base-metadata-in-dsa) 325 add select for Inherit type

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=dimensions/base
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type    | ref  | source     | access
$DATASET                   |         |      |            |
  |   |   | Location       |         | id   |            |
  |   |   |   | id         | integer |      |            | open
  |   |   |   | name       | string  |      |            | open
  |   |   |   | population | integer |      |            | open
  |   | Location           |         | name |            |
  |   |   | City           |         | id   | CITY       |
  |   |   |   | id         | integer |      | ID         | open
  |   |   |   | name       |         |      | NAME       | open
  |   |   |   | population |         |      | POPULATION | open
EOF

poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Location" $AUTH \
    id:=42 \
    name="Vilnius" \
    population:=625349

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

http POST "$SERVER/$DATASET/City" $AUTH \
    _id="dc918748-0e0c-47ba-9090-c878eabd7fb9" \
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
