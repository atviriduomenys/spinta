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
d | r | b | m | property    | type                           | ref     | source                                                       | prepare
$DATASET                    |                                |         |                                                              |
  | stations                | json                           |         | https://api.meteo.lt/v1/stations                             |
  |   |   | Station         |                                |         | .                                                            |
  |   |   |   | code        | string unique required         |         | code                                                         |
  |   |   |   | name        | string required                |         | name                                                         |
  |   |   |   | latitude    | number required                |         | coordinates.latitude                                         |
  |   |   |   | longitude   | number required                |         | coordinates.longitude                                        |
  |   |   |   | coordinates | geometry(point, 4326) required |         |                                                              | point(latitude, longitude)
  | observations            | json                           |         | https://api.meteo.lt/v1/stations/{station.code}/observations |
  |   |   |   |             | param                          | station | Station                                                      | select(code)
EOF
poetry run spinta inspect $BASEDIR/manifest.txt
#| UnknownParameter: Unknown parameter 'params'.
#|   Context:
#|     component: spinta.datasets.components.Resource
#|     manifest: default
#|     dataset: manifests/json
#|     resource: observations
#|     parameter: params
# TODO: https://github.com/atviriduomenys/spinta/issues/455


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type                           | ref     | source                                                            | prepare
$DATASET                    |                                |         |                                                                   |
  | stations                | json                           |         | https://api.meteo.lt/v1/stations                                  |
  |   |   | Station         |                                |         | .                                                                 |
  |   |   |   | code        | string unique required         |         | code                                                              |
  |   |   |   | name        | string required                |         | name                                                              |
  |   |   |   | latitude    | number required                |         | coordinates.latitude                                              |
  |   |   |   | longitude   | number required                |         | coordinates.longitude                                             |
  |   |   |   | coordinates | geometry(point, 4326) required |         |                                                                   | point(latitude, longitude)
  | observations            | json                           |         | https://api.meteo.lt/v1/stations/vilniaus-ams/observations/latest |
EOF
poetry run spinta inspect $BASEDIR/manifest.txt
#| │ /home/sirex/dev/data/spinta/spinta/manifests/dict/helpers.py:143 in nested_prop_names            │
#| │                                                                                                  │
#| │   140                                                                                            │
#| │   141                                                                                            │
#| │   142 def nested_prop_names(new_values: list, values: dict, root: str, seperator: str):          │
#| │ ❱ 143 │   for key, value in values.items():                                                      │
#| │   144 │   │   if isinstance(value, dict):                                                        │
#| │   145 │   │   │   nested_prop_names(new_values, values, f'{root}{seperator}{key}', seperator)    │
#| │   146 │   │   elif isinstance(value, list):                                                      │
#| │                                                                                                  │
#| │ ╭─────────────────────────────────────────── locals ───────────────────────────────────────────╮ │
#| │ │ new_values = [                                                                               │ │
#| │ │              │   'station.code',                                                             │ │
#| │ │              │   'station.name',                                                             │ │
#| │ │              │   'station.coordinates.code',                                                 │ │
#| │ │              │   'station.coordinates.name',                                                 │ │
#| │ │              │   'station.coordinates.coordinates.code',                                     │ │
#| │ │              │   'station.coordinates.coordinates.name',                                     │ │
#| │ │              │   'station.coordinates.coordinates.coordinates.code',                         │ │
#| │ │              │   'station.coordinates.coordinates.coordinates.name',                         │ │
#| │ │              │   'station.coordinates.coordinates.coordinates.coordinates.code',             │ │
#| │ │              │   'station.coordinates.coordinates.coordinates.coordinates.name',             │ │
#| │ │              │   ... +1938                                                                   │ │
#| │ │              ]                                                                               │ │
#| │ │       root = 'station.coordinates.coordinates.coordinates.coordinates.coordinates.coordinat… │ │
#| │ │  seperator = '.'                                                                             │ │
#| │ │     values = {                                                                               │ │
#| │ │              │   'code': 'vilniaus-ams',                                                     │ │
#| │ │              │   'name': 'Vilniaus AMS',                                                     │ │
#| │ │              │   'coordinates': {'latitude': 54.625992, 'longitude': 25.107064}              │ │
#| │ │              }                                                                               │ │
#| │ ╰──────────────────────────────────────────────────────────────────────────────────────────────╯ │
#| ╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
#| RecursionError: maximum recursion depth exceeded while calling a Python object
