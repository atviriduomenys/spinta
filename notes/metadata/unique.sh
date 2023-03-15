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
#| ,,,,,name,string ,,,,,open,,,
#| ,,,,,country,string ,,,,,open,,,
# FIXME: Space symbols should not be added after `string`.
poetry run spinta show
#| InvalidManifestFile:
#| Error while parsing '7' manifest entry:
#| Unknown 'string ' type of 'name' property in 'metadata/unique/City' model.
#|   Context:
#|     component: spinta.components.Property
#|     manifest: default
#|     schema: 7
#|     model: metadata/unique/City
#|     entity: 
#|     property: name
#|     attribute: 
#|     eid: 7
#|     error: Unknown 'string ' type of 'name' property in 'metadata/unique/City' model.
# FIXME: Can't load manifest after unique.
