INSTANCE=dimensions/param
DATASET=$INSTANCE
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type    | ref     | source   | prepare
$DATASET                   |         |         |          |
  | resource1              |         | default |          | sql
                           | param   | country |          | 'lt'
                           | param   | name    | Location | select(name)
                           | param   | date    |          | range(station.start_time, station.end_time, freq: 'd')
                           |         |         |          |
  |   |   | Location       |         |         |          |
                           | param   | country |          | 'lt'
                           |         |         |          | 'lv'
                           |         |         |          | 'ee'
                           |         |         | es       |
                           | param   | test    | Location | select(name)
  |   |   |   | id         | integer |         |          |
  |   |   |   | name       | string  |         |          |
  |   |   |   | population | integer |         |          |
EOF
poetry run spinta show $BASEDIR/manifest.txt
poetry run spinta check $BASEDIR/manifest.txt
