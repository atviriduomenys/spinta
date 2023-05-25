# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=dimensions/base/level3
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | level | access
$DATASET                 |         |         |       |
  |   |   | Location     |         | id      | 4     |
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   |          |         |         |       |     
  |   | Location         |         | id      | 4     |
  |   |   | Country      |         | id      | 4     |
  |   |   |   | id       |         |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   |          |         |         |       |     
  |   | Location         |         | name    | 3     |
  |   |   | City         |         | id      | 4     |
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   | country  | ref     | Country | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

poetry run spinta bootstrap
psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|               Table "public.dimensions/base/level3/Country"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _txn      | uuid                        |           |          | 
#|  _created  | timestamp without time zone |           |          | 
#|  _updated  | timestamp without time zone |           |          | 
#|  _id       | uuid                        |           | not null | 
#|  _revision | text                        |           |          | 
#|  name      | text                        |           |          | 
#| Indexes:
#|     "dimensions/base/level3/Country_pkey" PRIMARY KEY, btree (_id)
#|     "ix_dimensions/base/level3/Country__txn" btree (_txn)
#| Foreign-key constraints:
#|     "fk_dimensions/base/level3/Location_id"
#|         FOREIGN KEY (_id)
#|         REFERENCES "dimensions/base/level3/Location"(_id)
#| Referenced by:
#|     TABLE ""dimensions/base/level3/City""
#|         CONSTRAINT "fk_dimensions/base/level3/City_country._id"
#|             FOREIGN KEY ("country._id")
#|             REFERENCES "dimensions/base/level3/Country"(_id)
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/City"'
#|                  Table "public.dimensions/base/level3/City"
#|    Column    |            Type             | Collation | Nullable | Default 
#| -------------+-----------------------------+-----------+----------+---------
#|  _txn        | uuid                        |           |          | 
#|  _created    | timestamp without time zone |           |          | 
#|  _updated    | timestamp without time zone |           |          | 
#|  _id         | uuid                        |           | not null | 
#|  _revision   | text                        |           |          | 
#|  id          | integer                     |           |          | 
#|  name        | text                        |           |          | 
#|  country._id | uuid                        |           |          | 
#| Indexes:
#|     "dimensions/base/level3/City_pkey" PRIMARY KEY, btree (_id)
#|     "ix_dimensions/base/level3/City__txn" btree (_txn)
#| Foreign-key constraints:
#|     "fk_dimensions/base/level3/City_country._id"
#|         FOREIGN KEY ("country._id")
#|         REFERENCES "dimensions/base/level3/Country"(_id)
#|     "fk_dimensions/base/level3/Location_id"
#|         FOREIGN KEY (_id)
#|         REFERENCES "dimensions/base/level3/Location"(_id)
