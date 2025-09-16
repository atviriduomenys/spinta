# 2023-05-22 11:18

INSTANCE=manifests/json
DATASET=$INSTANCE
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR
unset SPINTA_CONFIG

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


http get https://api.meteo.lt/v1/stations/vilniaus-ams/observations/latest
#| {
#|     "station": {
#|         "code": "vilniaus-ams",
#|         "coordinates": {
#|             "latitude": 54.625992,
#|             "longitude": 25.107064
#|         },
#|         "name": "Vilniaus AMS"
#|     }
#|     "observations": [
#|         {
#|             "airTemperature": 19.2,
#|             "cloudCover": 0,
#|             "conditionCode": "clear",
#|             "feelsLikeTemperature": 19.2,
#|             "observationTimeUtc": "2023-05-24 06:00:00",
#|             "precipitation": 0,
#|             "relativeHumidity": 57,
#|             "seaLevelPressure": 1016.8,
#|             "windDirection": 349,
#|             "windGust": 2.2,
#|             "windSpeed": 0.9
#|         }
#|     ],
#| }

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
#| id | d | r | b | m | property                      | type                           | ref    | source                                                            | prepare                    | level | access | uri | title | description
#|    | manifests/json                                |                                |        |                                                                   |                            |       |        |     |       |
#|    |   | stations                                  | json                           |        | https://api.meteo.lt/v1/stations                                  |                            |       |        |     |       |
#|    |                                               |                                |        |                                                                   |                            |       |        |     |       |
#|    |   |   |   | Station                           |                                |        | .                                                                 |                            |       |        |     |       |
#|    |   |   |   |   | code                          | string unique required         |        | code                                                              |                            |       |        |     |       |
#|    |   |   |   |   | name                          | string required                |        | name                                                              |                            |       |        |     |       |
#|    |   |   |   |   | latitude                      | number required                |        | coordinates.latitude                                              |                            |       |        |     |       |
#|    |   |   |   |   | longitude                     | number required                |        | coordinates.longitude                                             |                            |       |        |     |       |
#|    |   |   |   |   | coordinates                   | geometry(point, 4326) required |        |                                                                   | point(latitude, longitude) |       |        |     |       |
#|    |   | observations                              | json                           |        | https://api.meteo.lt/v1/stations/vilniaus-ams/observations/latest |                            |       |        |     |       |
#|    |                                               |                                |        |                                                                   |                            |       |        |     |       |
#|    |   |   |   | Model1                            |                                |        | .                                                                 |                            |       |        |     |       |
#|    |   |   |   |   | station_code                  | string unique required         |        | station.code                                                      |                            |       |        |     |       |
#|    |   |   |   |   | station_name                  | string unique required         |        | station.name                                                      |                            |       |        |     |       |
#|    |   |   |   |   | station_coordinates_latitude  | number unique required         |        | station.coordinates.latitude                                      |                            |       |        |     |       |
#|    |   |   |   |   | station_coordinates_longitude | number unique required         |        | station.coordinates.longitude                                     |                            |       |        |     |       |
#|    |                                               |                                |        |                                                                   |                            |       |        |     |       |
#|    |   |   |   | Observations                      |                                |        | observations                                                      |                            |       |        |     |       |
#|    |   |   |   |   | observation_time_utc          | datetime unique required       |        | observationTimeUtc                                                |                            |       |        |     |       |
#|    |   |   |   |   | air_temperature               | number unique required         |        | airTemperature                                                    |                            |       |        |     |       |
#|    |   |   |   |   | feels_like_temperature        | number required                |        | feelsLikeTemperature                                              |                            |       |        |     |       |
#|    |   |   |   |   | wind_speed                    | number required                |        | windSpeed                                                         |                            |       |        |     |       |
#|    |   |   |   |   | wind_gust                     | number required                |        | windGust                                                          |                            |       |        |     |       |
#|    |   |   |   |   | wind_direction                | integer required               |        | windDirection                                                     |                            |       |        |     |       |
#|    |   |   |   |   | cloud_cover                   | integer required               |        | cloudCover                                                        |                            |       |        |     |       |
#|    |   |   |   |   | sea_level_pressure            | number required                |        | seaLevelPressure                                                  |                            |       |        |     |       |
#|    |   |   |   |   | relative_humidity             | integer required               |        | relativeHumidity                                                  |                            |       |        |     |       |
#|    |   |   |   |   | precipitation                 | boolean required               |        | precipitation                                                     |                            |       |        |     |       |
#|    |   |   |   |   | condition_code                | string required                |        | conditionCode                                                     |                            |       |        |     |       |
#|    |   |   |   |   | parent                        | ref                            | Model1 | ..                                                                |                            |       |        |     |       |

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property                | type                           | ref     | source                                                            | prepare
$DATASET                                |                                |         |                                                                   |
  | stations                            | json                           |         | https://api.meteo.lt/v1/stations                                  |
  |   |   | Station                     |                                | code    | .                                                                 |
  |   |   |   | code                    | string unique required         |         | code                                                              |
  |   |   |   | name                    | string required                |         | name                                                              |
  |   |   |   | latitude                | number required                |         | coordinates.latitude                                              |
  |   |   |   | longitude               | number required                |         | coordinates.longitude                                             |
  |   |   |   | coordinates             | geometry(point, 4326) required |         |                                                                   | point(latitude, longitude)
  | observations                        | json                           |         | https://api.meteo.lt/v1/stations/vilniaus-ams/observations/latest |
  |   | Station                         |                                | code    |                                                                   |
  |   |   | StationObservations         |                                | code    | .                                                                 |
  |   |   |   | code                    | string unique required         |         | station.code                                                      |
  |   |   |   | name                    | string required                |         | station.name                                                      |
  |   |   |   | latitude                | number required                |         | station.coordinates.latitude                                      |
  |   |   |   | longitude               | number required                |         | station.coordinates.longitude                                     |
                                        |                                |         |                                                                   |
  |   |   | Observation                 |                                |         | observations                                                      |
  |   |   |   | time                    | datetime required              | S       | observationTimeUtc                                                |
  |   |   |   | air_temperature         | number required                | °C      | airTemperature                                                    |
  |   |   |   | feels_like_temperature  | number required                | °C      | feelsLikeTemperature                                              |
  |   |   |   | wind_speed              | number required                | m/s     | windSpeed                                                         |
  |   |   |   | wind_gust               | number required                | m/s     | windGust                                                          |
  |   |   |   | wind_direction          | integer required               |         | windDirection                                                     |
  |   |   |   | cloud_cover             | integer required               | %       | cloudCover                                                        |
  |   |   |   | sea_level_pressure      | number required                |         | seaLevelPressure                                                  |
  |   |   |   | relative_humidity       | integer required               |         | relativeHumidity                                                  |
  |   |   |   | precipitation           | boolean required               |         | precipitation                                                     |
  |   |   |   | condition_code          | string required                |         | conditionCode                                                     |
  |   |   |   | station                 | ref                            | Station | ..station.code                                                                |
EOF
poetry run spinta inspect $BASEDIR/manifest.txt
#| id | d | r | b | m | property               | type                           | ref                                                  | source                                                            | prepare                    | level | access | uri | title | description
#|    | manifests/json                         |                                |                                                      |                                                                   |                            |       |        |     |       |
#|    |   | stations                           | json                           |                                                      | https://api.meteo.lt/v1/stations                                  |                            |       |        |     |       |
#|    |                                        |                                |                                                      |                                                                   |                            |       |        |     |       |
#|    |   |   |   | Station                    |                                | code                                                 | .                                                                 |                            |       |        |     |       |
#|    |   |   |   |   | code                   | string unique required         |                                                      | code                                                              |                            |       |        |     |       |
#|    |   |   |   |   | name                   | string required                |                                                      | name                                                              |                            |       |        |     |       |
#|    |   |   |   |   | latitude               | number required                |                                                      | coordinates.latitude                                              |                            |       |        |     |       |
#|    |   |   |   |   | longitude              | number required                |                                                      | coordinates.longitude                                             |                            |       |        |     |       |
#|    |   |   |   |   | coordinates            | geometry(point, 4326) required |                                                      |                                                                   | point(latitude, longitude) |       |        |     |       |
#|    |   | observations                       | json                           |                                                      | https://api.meteo.lt/v1/stations/vilniaus-ams/observations/latest |                            |       |        |     |       |
#|    |                                        |                                |                                                      |                                                                   |                            |       |        |     |       |
#|    |   |   | Station                        |                                | code                                                 |                                                                   |                            |       |        |     |       |
#|    |   |   |   | StationObservations        |                                | code                                                 | .                                                                 |                            |       |        |     |       |
#|    |   |   |   |   | code                   | string unique required         |                                                      | station.code                                                      |                            |       |        |     |       |
#|    |   |   |   |   | name                   | string required                |                                                      | station.name                                                      |                            |       |        |     |       |
#|    |   |   |   |   | latitude               | number required                |                                                      | station.coordinates.latitude                                      |                            |       |        |     |       |
#|    |   |   |   |   | longitude              | number required                |                                                      | station.coordinates.longitude                                     |                            |       |        |     |       |
#|    |                                        |                                |                                                      |                                                                   |                            |       |        |     |       |
#|    |   |   |   | Observation                |                                |                                                      | observations                                                      |                            |       |        |     |       |
#|    |   |   |   |   | time                   | datetime required              | S                                                    | observationTimeUtc                                                |                            |       |        |     |       |
#|    |   |   |   |   | air_temperature        | number required                | °C                                                   | airTemperature                                                    |                            |       |        |     |       |
#|    |   |   |   |   | feels_like_temperature | number required                | °C                                                   | feelsLikeTemperature                                              |                            |       |        |     |       |
#|    |   |   |   |   | wind_speed             | number required                | m/s                                                  | windSpeed                                                         |                            |       |        |     |       |
#|    |   |   |   |   | wind_gust              | number required                | m/s                                                  | windGust                                                          |                            |       |        |     |       |
#|    |   |   |   |   | wind_direction         | integer required               |                                                      | windDirection                                                     |                            |       |        |     |       |
#|    |   |   |   |   | cloud_cover            | integer required               | %                                                    | cloudCover                                                        |                            |       |        |     |       |
#|    |   |   |   |   | sea_level_pressure     | number required                |                                                      | seaLevelPressure                                                  |                            |       |        |     |       |
#|    |   |   |   |   | relative_humidity      | integer required               |                                                      | relativeHumidity                                                  |                            |       |        |     |       |
#|    |   |   |   |   | precipitation          | boolean required               |                                                      | precipitation                                                     |                            |       |        |     |       |
#|    |   |   |   |   | condition_code         | string required                |                                                      | conditionCode                                                     |                            |       |        |     |       |
#|    |   |   |   |   | station                | ref                            | Station                                              |                                                                   |                            |       |        |     |       |
#|    |   |   |   |   | parent                 | ref                            | StationObservations[code, latitude, longitude, name] | ..                                                                |                            |       |        |     |       |
# TODO: Would be nice to fix this.
