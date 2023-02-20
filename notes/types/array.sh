 # 2023-02-16 17:11

git log -1 --oneline
#| b4be7d3 (HEAD -> 161-add-support-for-array-type-in-tabular-manifests, origin/161-add-support-for-array-type-in-tabular-manifests) fix overshadow names

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/array
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref      | access
$DATASET                    |         |          |
                            |         |          |
  |   |   | Language        |         |          |
  |   |   |   | name        | string  |          | open
  |   |   |   | countries[] | backref | Country  | open
                            |         |          |
  |   |   | Country         |         |          |
  |   |   |   | name        | string  |          | open
  |   |   |   | languages[] | ref     | Language | open
EOF

poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client
