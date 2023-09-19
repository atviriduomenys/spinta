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
#| Traceback (most recent call last):
#|   File "spinta/cli/show.py", line 24, in show
#|     store = prepare_manifest(context, verbose=False)
#|             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/cli/helpers/store.py", line 138, in prepare_manifest
#|     commands.prepare(context, store.manifest)
#|   File "spinta/manifests/commands/init.py", line 14, in prepare
#|     commands.prepare(context, backend, manifest)
#|   File "spinta/backends/postgresql/commands/init.py", line 27, in prepare
#|     commands.prepare(context, backend, model)
#|   File "spinta/backends/postgresql/commands/init.py", line 61, in prepare
#|     main_table = sa.Table(
#|                  ^^^^^^^^^
#|   File "sqlalchemy/sql/schema.py", line 3259, in _set_parent
#|     for col in self._col_expressions(table):
#|                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "sqlalchemy/sql/schema.py", line 3253, in _col_expressions
#|     return [
#|            ^
#|   File "sqlalchemy/sql/schema.py", line 3254, in <listcomp>
#|     table.c[col] if isinstance(col, util.string_types) else col
#|     ~~~~~~~^^^^^
#|   File "sqlalchemy/sql/base.py", line 1192, in __getitem__
#|     return self._index[key]
#|            ~~~~~~~~~~~^^^^^
#| KeyError: 'id'
# TODO: Error is caused by Country.id, which is inherited, but also used as
#       Country's primary key. If model has a base, then model's model.ref must
#       be an inherited property, because in order to join model and base, a
#       property from base must be used.

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | level | access
$DATASET                 |         |         |       |
  |   |   | Location     |         | id      | 4     |
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   |          |         |         |       |     
  |   | Location         |         | id      | 4     |
  |   |   | Country      |         | id      | 4     |
  |   |   |   | id       | integer |         | 4     | open
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
#|     "dimensions/base/level3/Country_id_key" UNIQUE CONSTRAINT, btree (id)
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
#|     "dimensions/base/level3/City_id_key" UNIQUE CONSTRAINT, btree (id)
#|     "ix_dimensions/base/level3/City__txn" btree (_txn)
#| Foreign-key constraints:
#|     "fk_dimensions/base/level3/City_country._id"
#|         FOREIGN KEY ("country._id")
#|         REFERENCES "dimensions/base/level3/Country"(_id)

# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

LTU=37f3be0c-6533-48d5-8ee5-4fab07ad18ae
VLN=9dc732fb-7df2-454c-8bf1-04e35d5f651b

http POST "$SERVER/$DATASET/Location" $AUTH _id=$LTU name=Lithuania
http POST "$SERVER/$DATASET/Location" $AUTH _id=$VLN name=Vilnius

http POST "$SERVER/$DATASET/Country" $AUTH _id=$LTU name=Lithuania
http POST "$SERVER/$DATASET/City" $AUTH _id=$VLN 'country[_id]'=$LTU name=Vilnius


http GET "$SERVER/$DATASET/Location?format(ascii)"
#| id  name     
#| --  ---------
#| ∅   Lithuania
#| ∅   Vilnius

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| id  name     
#| --  ---------
#| ∅   Lithuania

http GET "$SERVER/$DATASET/City?format(ascii)"
#| id  name     country._id                         
#| --  -------  ------------------------------------
#| ∅   Vilnius  37f3be0c-6533-48d5-8ee5-4fab07ad18ae
