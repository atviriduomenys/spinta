# 2023-03-09 09:36

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=dimensions/dataset
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref | source | access
$DATASET                 |         |     |        |
  |   |   | City         |         | id  | CITY   |
  |   |   |   | id       | integer |     | ID     | open
  |   |   |   | name     | string  |     | NAME   | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
poetry run spinta show
poetry run spinta check
poetry run spinta check --check-filename
# FIXME: This should raise an error, because here filename is `manifest.csv`
#        and dataset name is `dimensions/dataset`, names do not match, so this
#        should be an error.
