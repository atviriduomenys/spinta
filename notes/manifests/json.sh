# 2023-05-22 11:18

INSTANCE=manifests/json
DATASET=$INSTANCE
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR

poetry run spinta inspect -r json https://api.meteo.lt/v1/stations
#| id | d | r | b | m | property              | type                   | ref | source                           | prepare | level | access | uri | title | description
#|    | dataset                               |                        |     |                                  |         |       |        |     |       |
#|    |   | resource                          | json                   |     | https://api.meteo.lt/v1/stations |         |       |        |     |       |
#|    |                                       |                        |     |                                  |         |       |        |     |       |
#|    |   |   |   | Model1                    |                        |     | .                                |         |       |        |     |       |
#|    |   |   |   |   | code                  | string unique required |     | code                             |         |       |        |     |       |
#|    |   |   |   |   | name                  | string unique required |     | name                             |         |       |        |     |       |
#|    |   |   |   |   | coordinates_latitude  | number unique required |     | coordinates.latitude             |         |       |        |     |       |
#|    |   |   |   |   | coordinates_longitude | number unique required |     | coordinates.longitude            |         |       |        |     |       |


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type                           | ref | source                           | prepare
$DATASET                    |                                |     |                                  |
  | stations                | json                           |     | https://api.meteo.lt/v1/stations |
  |   |   | Station         |                                |     | .                                |
  |   |   |   | code        | string unique required         |     | code                             |
  |   |   |   | name        | string required                |     | name                             |
  |   |   |   | latitude    | number required                |     | coordinates.latitude             |
  |   |   |   | longitude   | number required                |     | coordinates.longitude            |
  |   |   |   | coordinates | geometry(point, 4326) required |     |                                  | point(latitude, longitude)
EOF
poetry run spinta inspect $BASEDIR/manifest.txt
#| id | d | r | b | m | property    | type                           | ref | source                           | prepare                    | level | access | uri | title | description
#|    | manifests/json              |                                |     |                                  |                            |       |        |     |       |
#|    |   | stations                | json                           |     | https://api.meteo.lt/v1/stations |                            |       |        |     |       |
#|    |                             |                                |     |                                  |                            |       |        |     |       |
#|    |   |   |   | Station         |                                |     | .                                |                            |       |        |     |       |
#|    |   |   |   |   | code        | string unique required         |     | code                             |                            |       |        |     |       |
#|    |   |   |   |   | name        | string required                |     | name                             |                            |       |        |     |       |
#|    |   |   |   |   | latitude    | number required                |     | coordinates.latitude             |                            |       |        |     |       |
#|    |   |   |   |   | longitude   | number required                |     | coordinates.longitude            |                            |       |        |     |       |
#|    |   |   |   |   | coordinates | geometry(point, 4326) required |     |                                  | point(latitude, longitude) |       |        |     |       |


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type                           | ref     | source                                                      | prepare
$DATASET                    |                                |         |                                                             |
  | stations                | json                           |         | https://api.meteo.lt/v1/stations                            |
  |   |   | Station         |                                |         | .                                                           |
  |   |   |   | code        | string unique required         |         | code                                                        |
  |   |   |   | name        | string required                |         | name                                                        |
  |   |   |   | latitude    | number required                |         | coordinates.latitude                                        |
  |   |   |   | longitude   | number required                |         | coordinates.longitude                                       |
  |   |   |   | coordinates | geometry(point, 4326) required |         |                                                             | point(latitude, longitude)
  | observations            | json                           |         | https://api.meteo.lt/v1/stations{station.code}/observations |
  |   |   |   |             | param                          | station | Station                                                     | select(code)
EOF
poetry run spinta inspect $BASEDIR/manifest.txt
#| UnknownParameter: Unknown parameter 'params'.
#|   Context:
#|     component: spinta.datasets.components.Resource
#|     manifest: default
#|     dataset: manifests/json
#|     resource: observations
#|     parameter: params
# TODO: https://github.com/atviriduomenys/spinta/issues/210
# TODO: https://github.com/atviriduomenys/spinta/issues/256
