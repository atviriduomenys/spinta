# 2023-03-01 11:31

git log -1 --oneline
#| 0795a7e (HEAD -> 148-unique-constraint-in-tabular-manifests, origin/148-unique-constraint-in-tabular-manifests) fix

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
  |   |   |   |          | unique        | name, country | open
  |   |   |   | name     | string        |               | open
  |   |   |   | country  | string        |               | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
#| TabularManifestError:
#| var/instances/metadata/unique/manifest.txt:6:type:
#| Unknown additional dimension name unique.
