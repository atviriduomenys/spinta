INSTANCE=datasets/json
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

unset SPINTA_CONFIG

http -b get https://api.vilnius.lt/api/calendar/get-non-working-days
#| {
#|     "nonWorkingDates": [
#|         {
#|             "date": "2023-01-01",
#|             "name": "Naujieji metai"
#|         }
#|     ],
#|     "workingHolidayDates": [
#|         {
#|             "date": "2022-12-31",
#|             "name": "Naujųjų Metų išvakarės"
#|         }
#|     ]
#| }

poetry run spinta inspect -r json https://api.vilnius.lt/api/calendar/get-non-working-days
#| id | d | r | b | m | property            | type                   | ref | source                                                   | prepare | level | access | uri | title | description
#|    | dataset                             |                        |     |                                                          |         |       |        |     |       |
#|    |   | resource                        | json                   |     | https://api.vilnius.lt/api/calendar/get-non-working-days |         |       |        |     |       |
#|    |                                     |                        |     |                                                          |         |       |        |     |       |
#|    |   |   |   | NonWorkingDates         |                        |     | nonWorkingDates                                          |         |       |        |     |       |
#|    |   |   |   |   | date                | date required unique   |     | date                                                     |         |       |        |     |       |
#|    |   |   |   |   | name                | string required unique |     | name                                                     |         |       |        |     |       |
#|    |                                     |                        |     |                                                          |         |       |        |     |       |
#|    |   |   |   | WorkingHolidayDates     |                        |     | workingHolidayDates                                      |         |       |        |     |       |
#|    |   |   |   |   | date                | date required unique   |     | date                                                     |         |       |        |     |       |
#|    |   |   |   |   | name                | string required        |     | name                                                     |         |       |        |     |       |


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type                 | ref  | source                                                   | prepare | level | access
$DATASET                 |                      |      |                                                          |         |       |
  | resource             | json                 |      | https://api.vilnius.lt/api/calendar/get-non-working-days |         |       |
                         |                      |      |                                                          |         |       |
  |   |   | Holiday      |                      | date | nonWorkingDates                                          |         | 4     |
  |   |   |   | date     | date required unique |      | date                                                     |         | 4     | open
  |   |   |   | name     | string required      |      | name                                                     |         | 4     | open
                         |                      |      |                                                          |         |       |
  |   |   | Workday      |                      | date | workingHolidayDates                                      |         | 4     |
  |   |   |   | date     | date required unique |      | date                                                     |         | 4     | open
  |   |   |   | name     | string required      |      | name                                                     |         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Holiday?limit(10)&format(ascii)"
#| date        name                              
#| ----------  ----------------------------------
#| 2023-01-01  Naujieji metai
#| 2023-02-16  Lietuvos nepriklausomybės atkūrimo diena
#| 2023-03-11  Lietuvos valstybės atkūrimo diena
#| 2023-04-09  Velykos
#| 2023-04-10  antroji Velykų diena
#| 2023-05-01  Tarptautinė darbo diena
#| 2023-06-24  Joninės/Rasos
#| 2023-07-06  Valstybės (Lietuvos karaliaus Mindaugo karūnavimo) diena
#| 2023-08-15  Žolinė (Švč. Mergelės Marijos ėmimo į dangų diena)
#| 2023-11-01  Visų šventųjų diena
#| 2023-11-02  Mirusiųjų atminimo (Vėlinių) diena

http GET "$SERVER/$DATASET/Workday?select(date,name)&limit(10)&format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NotImplementedError",
#|             "message": "Could not find signature for and: <DaskDataFrameQueryBuilder, Expr>"
#|         }
#|     ]
#| }
# TODO: Implemente `select`.

http GET "$SERVER/$DATASET/Workday?limit(10)&format(ascii)"
#| date        name            
#| ----------  ----------------
#| 2022-12-31  Naujųjų Metų išvakarės
#| 2023-02-14  Valentino diena
#| 2023-02-21  Užgavėnės
#| 2023-03-26  Vasaros laikas prasideda
#| 2023-04-07  Didysis Penktadienis
#| 2023-04-08  Didysis Šeštadienis
#| 2023-10-29  Vasaros laikas baigiasi
#| 2023-10-31  Helovinas
#| 2023-12-31  Naujųjų Metų išvakarės


poetry run spinta inspect -r json "https://www.geoportal.lt/metadata-catalog/rest/find/document?start=1&max=10&f=pjson"
#| id | d | r | b | m | property             | type                     | ref     | source                                                                              | prepare | level | access | uri | title | description
#|    | dataset                              |                          |         |                                                                                     |         |       |        |     |       |
#|    |   | resource                         | json                     |         | https://www.geoportal.lt/metadata-catalog/rest/find/document?start=1&max=10&f=pjson |         |       |        |     |       |
#|    |                                      |                          |         |                                                                                     |         |       |        |     |       |
#|    |   |   |   | Model1                   |                          |         | .                                                                                   |         |       |        |     |       |
#|    |   |   |   |   | title                | string required          |         | title                                                                               |         |       |        |     |       |
#|    |   |   |   |   | description          | string required          |         | description                                                                         |         |       |        |     |       |
#|    |   |   |   |   | copyright            | string                   |         | copyright                                                                           |         |       |        |     |       |
#|    |   |   |   |   | provider             | url required             |         | provider                                                                            |         |       |        |     |       |
#|    |   |   |   |   | updated              | datetime required        |         | updated                                                                             |         |       |        |     |       |
#|    |   |   |   |   | source               | url required             |         | source                                                                              |         |       |        |     |       |
#|    |   |   |   |   | more                 | url required             |         | more                                                                                |         |       |        |     |       |
#|    |   |   |   |   | total_results        | integer required         |         | totalResults                                                                        |         |       |        |     |       |
#|    |   |   |   |   | start_index          | boolean required         |         | startIndex                                                                          |         |       |        |     |       |
#|    |   |   |   |   | items_per_page       | binary required          |         | itemsPerPage                                                                        |         |       |        |     |       |
#|    |                                      |                          |         |                                                                                     |         |       |        |     |       |
#|    |   |   |   | Records                  |                          |         | records                                                                             |         |       |        |     |       |
#|    |   |   |   |   | title                | string required unique   |         | title                                                                               |         |       |        |     |       |
#|    |   |   |   |   | id                   | string required unique   |         | id                                                                                  |         |       |        |     |       |
#|    |   |   |   |   | updated              | datetime required unique |         | updated                                                                             |         |       |        |     |       |
#|    |   |   |   |   | summary              | string required          |         | summary                                                                             |         |       |        |     |       |
#|    |   |   |   |   | bbox                 | array                    |         | bbox                                                                                |         |       |        |     |       |
#|    |   |   |   |   | geometry_type        | string required          |         | geometry.type                                                                       |         |       |        |     |       |
#|    |   |   |   |   | geometry_coordinates | array                    |         | geometry.coordinates                                                                |         |       |        |     |       |
#|    |   |   |   |   | parent               | ref                      | Model1  | ..                                                                                  |         |       |        |     |       |
#|    |                                      |                          |         |                                                                                     |         |       |        |     |       |
#|    |   |   |   | Links                    |                          |         | records[].links                                                                     |         |       |        |     |       |
#|    |   |   |   |   | href                 | string required          |         | href                                                                                |         |       |        |     |       |
#|    |   |   |   |   | type                 | string required          |         | type                                                                                |         |       |        |     |       |
#|    |   |   |   |   | records              | ref                      | Records | ..                                                                                  |         |       |        |     |       |

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property             | type                     | ref     | source                                                                              | prepare | level | access | uri | title | description
$DATASET                             |                          |         |                                                                                     |         |       |        |     |       |
  | resource                         | json                     |         | https://www.geoportal.lt/metadata-catalog/rest/find/document?start=1&max=10&f=pjson |         |       |        |     |       |
                                     |                          |         |                                                                                     |         |       |        |     |       |
  |   |   | Page                     |                          |         | .                                                                                   |         |       |        |     |       |
  |   |   |   | title                | string required          |         | title                                                                               |         |       | open   |     |       |
  |   |   |   | description          | string required          |         | description                                                                         |         |       | open   |     |       |
  |   |   |   | copyright            | string                   |         | copyright                                                                           |         |       | open   |     |       |
  |   |   |   | provider             | url required             |         | provider                                                                            |         |       | open   |     |       |
  |   |   |   | updated              | datetime required        |         | updated                                                                             |         |       | open   |     |       |
  |   |   |   | source               | url required             |         | source                                                                              |         |       | open   |     |       |
  |   |   |   | more                 | url required             |         | more                                                                                |         |       | open   |     |       |
  |   |   |   | total_results        | integer required         |         | totalResults                                                                        |         |       | open   |     |       |
  |   |   |   | start_index          | boolean required         |         | startIndex                                                                          |         |       | open   |     |       |
  |   |   |   | items_per_page       | binary required          |         | itemsPerPage                                                                        |         |       | open   |     |       |
                                     |                          |         |                                                                                     |         |       |        |     |       |
  |   |   | Records                  |                          |         | records                                                                             |         |       |        |     |       |
  |   |   |   | title                | string required unique   |         | title                                                                               |         |       | open   |     |       |
  |   |   |   | id                   | string required unique   |         | id                                                                                  |         |       | open   |     |       |
  |   |   |   | updated              | datetime required unique |         | updated                                                                             |         |       | open   |     |       |
  |   |   |   | summary              | string required          |         | summary                                                                             |         |       | open   |     |       |
  |   |   |   | bbox                 | array                    |         | bbox                                                                                |         |       | open   |     |       |
  |   |   |   | geometry_type        | string required          |         | geometry.type                                                                       |         |       | open   |     |       |
  |   |   |   | geometry_coordinates | array                    |         | geometry.coordinates                                                                |         |       | open   |     |       |
                                     |                          |         |                                                                                     |         |       |        |     |       |
  |   |   | Links                    |                          |         | records[].links                                                                     |         |       |        |     |       |
  |   |   |   | href                 | string required          |         | href                                                                                |         |       | open   |     |       |
  |   |   |   | type                 | string required          |         | type                                                                                |         |       | open   |     |       |
  |   |   |   | records              | ref                      | Records | ..                                                                                  |         |       | open   |     |       |
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

http GET "$SERVER/$DATASET/Page?format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "TypeError",
#|             "message": "Cannot use .astype to convert from timezone-aware dtype to timezone-naive dtype. Use obj.tz_localize(None) or obj.tz_convert('UTC').tz_localize(None) instead."
#|         }
#|     ]
#| }

tail -200 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 162, in create_http_response
#|     return await commands.getall(
#|            ^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/read.py", line 125, in getall
#|     return render(context, request, model, params, rows, action=action)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/formats/ascii/commands.py", line 28, in render
#|     return _render(
#|            ^^^^^^^^
#|   File "spinta/formats/ascii/commands.py", line 62, in _render
#|     aiter(peek_and_stream(fmt(
#|           ^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 232, in peek_and_stream
#|     peek = list(itertools.islice(stream, 2))
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/formats/ascii/components.py", line 41, in __call__
#|     peek = next(data, None)
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/accesslog/__init__.py", line 162, in log_response
#|     for row in rows:
#|   File "spinta/commands/read.py", line 110, in <genexpr>
#|     rows = (
#|            ^
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 371, in getall
#|     yield from _dask_get_all(context, query, df, backend, model, builder)
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 436, in _dask_get_all
#|     for i, row in qry.iterrows():
#|   File "pandas/core/dtypes/astype.py", line 116, in _astype_nansafe
#|     return dta.astype(dtype, copy=False)._ndarray
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "pandas/core/arrays/datetimes.py", line 682, in astype
#|     raise TypeError(
#| TypeError: Cannot use .astype to convert from timezone-aware dtype to timezone-naive dtype. Use obj.tz_localize(None) or obj.tz_convert('UTC').tz_localize(None) instead.
# FIXME:

http -b "https://www.geoportal.lt/metadata-catalog/rest/find/document?start=1&max=10&f=pjson"
#| {
#|     "copyright": "",
#|     "description": "Pastaruoju metu atnaujinti metaduomenų dokumentai.",
#|     "itemsPerPage": 10,
#|     "more": "https://www.geoportal.lt/metadata-catalog/rest/find/document?start=11&max=10&f=pjson",
#|     "provider": "https://www.geoportal.lt/metadata-catalog",
#|     "records": [
#|         ...
#|     ],
#|     "source": "https://www.geoportal.lt/metadata-catalog/rest/find/document?start=1&max=10&f=pjson",
#|     "startIndex": 1,
#|     "title": "Geoportal GeoRSS.",
#|     "totalResults": 762,
#|     "updated": "2023-07-09T11:35:13Z"
#| }
# FIXME: There is one date and it is timezone-aware, because of Z suffix, which
#        means it is in UTC timezone. Please handle timezone-aware dates correctly.


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property                                                                            | type                     | ref            | source                                                                                                                                                                               | prepare       | level | access | uri                                       | title | description
$DATASET                                                                                            |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
                                                                                                    | prefix                   | gmd            |                                                                                                                                                                                      |               |       |        | http://www.isotc211.org/2005/gmd          |       |
                                                                                                    |                          | gco            |                                                                                                                                                                                      |               |       |        | http://www.isotc211.org/2005/gco          |       |
                                                                                                    |                          | srv            |                                                                                                                                                                                      |               |       |        | http://www.isotc211.org/2005/srv          |       |
                                                                                                    |                          | gml            |                                                                                                                                                                                      |               |       |        | http://www.opengis.net/gml                |       |
                                                                                                    |                          | xlink          |                                                                                                                                                                                      |               |       |        | http://www.w3.org/1999/xlink              |       |
                                                                                                    |                          | xsi            |                                                                                                                                                                                      |               |       |        | http://www.w3.org/2001/XMLSchema-instance |       |
                                                                                                    |                          | gts            |                                                                                                                                                                                      |               |       |        | http://www.isotc211.org/2005/gts          |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  | records                                                                                         | json                     |                | {url}                                                                                                                                                                                |               |       |        |                                           |       |
                                                                                                    | param                    | url            | https://www.geoportal.lt/metadata-catalog/rest/find/document?start=1&max=10&f=pjson                                                                                                  |               |       |        |                                           |       |
                                                                                                    |                          |                | Page                                                                                                                                                                                 | select().more |       |        |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Page                                                                                    |                          | start_index    | .                                                                                                                                                                                    |               |       |        |                                           |       |
  |   |   |   | title                                                                               | string required          |                | title                                                                                                                                                                                |               |       | open   |                                           |       |
  |   |   |   | description                                                                         | string required          |                | description                                                                                                                                                                          |               |       | open   |                                           |       |
  |   |   |   | copyright                                                                           | string                   |                | copyright                                                                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | provider                                                                            | url required             |                | provider                                                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | source                                                                              | url required             |                | source                                                                                                                                                                               |               |       | open   |                                           |       |
  |   |   |   | more                                                                                | url required             |                | more                                                                                                                                                                                 |               |       | open   |                                           |       |
  |   |   |   | total_results                                                                       | integer required         |                | totalResults                                                                                                                                                                         |               |       | open   |                                           |       |
  |   |   |   | start_index                                                                         | boolean required         |                | startIndex                                                                                                                                                                           |               |       | open   |                                           |       |
  |   |   |   | items_per_page                                                                      | binary required          |                | itemsPerPage                                                                                                                                                                         |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | PageDataset                                                                             |                          | id             | records                                                                                                                                                                              |               |       |        |                                           |       |
  |   |   |   | title                                                                               | string required unique   |                | title                                                                                                                                                                                |               |       | open   |                                           |       |
  |   |   |   | id                                                                                  | string required unique   |                | id                                                                                                                                                                                   |               |       | open   |                                           |       |
  |   |   |   | updated                                                                             | datetime required unique |                | updated                                                                                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | summary                                                                             | string required          |                | summary                                                                                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | bbox                                                                                | array                    |                | bbox                                                                                                                                                                                 |               |       | open   |                                           |       |
  |   |   |   | geometry_type                                                                       | string required          |                | geometry.type                                                                                                                                                                        |               |       | open   |                                           |       |
  |   |   |   | geometry_coordinates                                                                | array                    |                | geometry.coordinates                                                                                                                                                                 |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Link                                                                                    |                          | href           | records[].links                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   |   | href                                                                                | string required          |                | href                                                                                                                                                                                 |               |       | open   |                                           |       |
  |   |   |   | type                                                                                | string required          |                | type                                                                                                                                                                                 |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  | record                                                                                          | xml                      |                | https://www.geoportal.lt/metadata-catalog/rest/document?id={id}                                                                                                                      |               |       |        |                                           |       |
                                                                                                    | param                    | id             | PageDataset                                                                                                                                                                          | select().id   |       |        |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | MetaData                                                                                | id                       |                | /                                                                                                                                                                                    |               |       |        |                                           |       |
  |   |   |   | id                                                                                  | string required unique   |                | gmd:MD_Metadata/gmd:fileIdentifier/gco:CharacterString                                                                                                                               |               |       | open   |                                           |       |
  |   |   |   | xsi_schema_location                                                                 | url required unique      |                | gmd:MD_Metadata/@xsi:schemaLocation                                                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | language                                                                            | ref                      | Language       | gmd:MD_Metadata/gmd:language/gmd:LanguageCode/@codeListValue                                                                                                                         |               |       | open   |                                           |       |
  |   |   |   | charset                                                                             | ref                      | Charset        | gmd:MD_Metadata/gmd:characterSet/gmd:MD_CharacterSetCode/@codeListValue                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | hierarchy                                                                           | ref                      | Hierarchy      | gmd:MD_Metadata/gmd:hierarchyLevel/gmd:MD_ScopeCode/@codeListValue                                                                                                                   |               |       | open   |                                           |       |
  |   |   |   | representative                                                                      | ref                      | Representative | gmd:MD_Metadata/gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString                                                                                         |               |       | open   |                                           |       |
  |   |   |   | date_stamp                                                                          | date required            |                | gmd:MD_Metadata/gmd:dateStamp/gco:Date                                                                                                                                               |               |       | open   |                                           |       |
  |   |   |   | metadata_standard_name                                                              | string required unique   |                | gmd:MD_Metadata/gmd:metadataStandardName/gco:CharacterString                                                                                                                         |               |       | open   |                                           |       |
  |   |   |   | metadata_standard_version                                                           | number required unique   |                | gmd:MD_Metadata/gmd:metadataStandardVersion/gco:CharacterString                                                                                                                      |               |       | open   |                                           |       |
  |   |   |   | locale                                                                              | ref                      | Locale         | gmd:MD_Metadata/gmd:locale/gmd:PT_Locale/@id                                                                                                                                         |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Language                                                                                |                          | value          | /gmd:MD_Metadata/gmd:language/gmd:LanguageCode                                                                                                                                       |               |       |        |                                           |       |
  |   |   |   | space                                                                               | string                   |                | @codeSpace                                                                                                                                                                           |               |       | open   |                                           |       |
  |   |   |   | list                                                                                | url                      |                | @codeList                                                                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | value                                                                               | string required unique   |                | @codeListValue                                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | text                                                                                | string required          |                | #text                                                                                                                                                                                |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Charset                                                                                 |                          | value          | /gmd:MD_Metadata/gmd:characterSet/gmd:MD_CharacterSetCode                                                                                                                            |               |       |        |                                           |       |
  |   |   |   | space                                                                               | string                   |                | /@codeSpace                                                                                                                                                                          |               |       | open   |                                           |       |
  |   |   |   | list                                                                                | url                      |                | @codeList                                                                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | value                                                                               | string required unique   |                | @codeListValue                                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | text                                                                                | string required          |                | #text                                                                                                                                                                                |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Locale                                                                                  |                          | id             | /gmd:MD_Metadata/gmd:locale/gmd:PT_Locale                                                                                                                                            |               |       |        |                                           |       |
  |   |   |   | id                                                                                  | string required unique   |                | @id                                                                                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | language                                                                            | ref                      | LocaleLanguage | gmd:languageCode/gmd:LanguageCode/@codeListValue                                                                                                                                     |               |       | open   |                                           |       |
  |   |   |   | charset                                                                             | ref                      | LocaleCharset  | gmd:characterEncoding/gmd:MD_CharacterSetCode/@codeListValue                                                                                                                         |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   | Language                                                                                    |                          | value          |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | LocaleLanguage                                                                          |                          | value          | /gmd:MD_Metadata/gmd:locale/gmd:PT_Locale/gmd:languageCode/gmd:LanguageCode                                                                                                          |               |       | open   |                                           |       |
  |   |   |   | list                                                                                | url                      |                | @codeList                                                                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | value                                                                               | string required unique   |                | @codeListValue                                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | text                                                                                | string required          |                | #text                                                                                                                                                                                |               |       | open   |                                           |       |
  |   | /                                                                                           |                          |                |                                                                                                                                                                                      |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   | Charset                                                                                     |                          | value          |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | LocaleCharset                                                                           |                          | value          | /gmd:MD_Metadata/gmd:locale/gmd:PT_Locale/gmd:characterEncoding/gmd:MD_CharacterSetCode                                                                                              |               |       | open   |                                           |       |
  |   |   |   | list                                                                                | url                      |                | @codeList                                                                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | value                                                                               | string required unique   |                | @codeListValue                                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | text                                                                                | string required          |                | #text                                                                                                                                                                                |               |       | open   |                                           |       |
  |   | /                                                                                           |                          |                |                                                                                                                                                                                      |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Hierarchy                                                                               |                          | value          | /gmd:MD_Metadata/gmd:hierarchyLevel/gmd:MD_ScopeCode                                                                                                                                 |               |       |        |                                           |       |
  |   |   |   | list                                                                                | url                      |                | @codeList                                                                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | value                                                                               | string required unique   |                | @codeListValue                                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | text                                                                                | string required          |                | #text                                                                                                                                                                                |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Representative                                                                          | value                    |                | /gmd:MD_Metadata/gmd:contact/gmd:CI_ResponsibleParty                                                                                                                                 |               |       |        |                                           |       |
  |   |   |   | organization                                                                        | ref                      | Organization   | gmd:organisationName/gco:CharacterString                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | contact_ci_responsible_party_role_ci_role_code_code_space                           | string required unique   |                | gmd:role/gmd:CI_RoleCode/@codeSpace                                                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | contact_ci_responsible_party_role_ci_role_code_code_list                            | url required unique      |                | gmd:role/gmd:CI_RoleCode/@codeList                                                                                                                                                   |               |       | open   |                                           |       |
  |   |   |   | contact_ci_responsible_party_role_ci_role_code_code_list_value                      | string required unique   |                | gmd:role/gmd:CI_RoleCode/@codeListValue                                                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | contact_ci_responsible_party_role_ci_role_code_text                                 | string required unique   |                | gmd:role/gmd:CI_RoleCode/#text                                                                                                                                                       |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Role                                                                                    |                          | code           | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty/gmd:role/gmd:CI_RoleCode                                             |               |       |        |                                           |       |
  |   |   |   | type                                                                                | string required          |                | gmd:role/gmd:CI_RoleCode/@codeSpace                                                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | uri                                                                                 | url required             |                | gmd:role/gmd:CI_RoleCode/@codeList                                                                                                                                                   |               |       | open   |                                           |       |
  |   |   |   | code                                                                                | string required          |                | gmd:role/gmd:CI_RoleCode/@codeListValue                                                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | name                                                                                | string required          |                | gmd:role/gmd:CI_RoleCode/#text                                                                                                                                                       |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Organization                                                                            |                          | name           | /gmd:MD_Metadata/gmd:contact/gmd:CI_ResponsibleParty                                                                                                                                 |               |       |        |                                           |       |
  |   |   |   | xsi_type                                                                            | string                   |                | gmd:organisationName/@xsi:type                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | name                                                                                | string required          |                | gmd:organisationName/gco:CharacterString                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | localized_name_locale                                                               | string                   |                | gmd:organisationName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/@locale                                                                                              |               |       | open   |                                           |       |
  |   |   |   | localized_name                                                                      | string                   |                | gmd:organisationName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/#text                                                                                                |               |       | open   |                                           |       |
  |   |   |   | phone                                                                               | string unique            |                | gmd:contactInfo/gmd:CI_Contact/gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString                                                                                              |               |       | open   |                                           |       |
  |   |   |   | email                                                                               | string unique            |                | gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString                                                                              |               |       | open   |                                           |       |
  |   |   |   | city                                                                                | string                   |                | gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:city/gco:CharacterString                                                                                               |               |       | open   |                                           |       |
  |   |   |   | postal_code                                                                         | integer                  |                | gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:postalCode/gco:CharacterString                                                                                         |               |       | open   |                                           |       |
  |   |   |   | country                                                                             | string                   |                | gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:country/gco:CharacterString                                                                                            |               |       | open   |                                           |       |
  |   |   |   | website                                                                             | url                      |                | gmd:contactInfo/gmd:CI_Contact/gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL                                                                                          |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Person                                                                                  |                          | name           | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty                                                                      |               |       |        |                                           |       |
  |   |   |   | name                                                                                | string required          |                | gmd:individualName/gco:CharacterString                                                                                                                                               |               |       | open   |                                           |       |
  |   |   |   | organization                                                                        | string required          |                | gmd:organisationName/gco:CharacterString                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | xsi_type                                                                            | string required unique   |                | gmd:individualName/@xsi:type                                                                                                                                                         |               |       | open   |                                           |       |
  |   |   |   | locale                                                                              | string required unique   |                | gmd:individualName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/@locale                                                                                                |               |       | open   |                                           |       |
  |   |   |   | locale_name                                                                         | string required unique   |                | gmd:individualName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/#text                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | email                                                                               | string required          |                | gmd:contactInfo/gmd:CI_Contact/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString                                               |               |       | open   |                                           |       |
  |   |   |   | phone                                                                               | string required          |                | gmd:contactInfo/gmd:CI_Contact/gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString                                                                                              |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   | Representative                                                                              |                          | organization   |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | DatasetRepresentative                                                                   |                          | organization   | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty                                                                      |               |       | open   |                                           |       |
  |   |   |   | xsi_type                                                                            | string                   |                | gmd:organisationName/@xsi:type                                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | organization                                                                        | string required          |                | gmd:organisationName/gco:CharacterString                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | representative                                                                      | string                   |                | gmd:organisationName/gco:CharacterString                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | role                                                                                | ref                      | Role           | gmd:role/gmd:CI_RoleCode/@codeListValue                                                                                                                                              |               |       | open   |                                           |       |
  |   | /                                                                                           |                          |                |                                                                                                                                                                                      |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   | Organization                                                                                |                          | name           |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | DatasetOrganization                                                                     |                          | name           | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty                                                                      |               |       | open   |                                           |       |
  |   |   |   | name                                                                                | string required          |                | gmd:organisationName/gco:CharacterString                                                                                                                                             |               |       | open   |                                           |       |
  |   |   |   | localized_name_locale                                                               | string required          |                | gmd:organisationName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/@locale                                                                                              |               |       | open   |                                           |       |
  |   |   |   | localized_name                                                                      | string required          |                | gmd:organisationName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/#text                                                                                                |               |       | open   |                                           |       |
  |   |   |   | city                                                                                | string required          |                | gmd:contactInfo/gmd:CI_Contact/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:city/gco:CharacterString                                                                |               |       | open   |                                           |       |
  |   |   |   | postal_code                                                                         | integer required         |                | gmd:contactInfo/gmd:CI_Contact/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:postalCode/gco:CharacterString                                                          |               |       | open   |                                           |       |
  |   |   |   | country                                                                             | string required          |                | gmd:contactInfo/gmd:CI_Contact/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:country/gco:CharacterString                                                             |               |       | open   |                                           |       |
  |   |   |   | website                                                                             | url required             |                | gmd:contactInfo/gmd:CI_Contact/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:contactInfo/gmd:CI_Contact/gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL |               |       | open   |                                           |       |
  |   | /                                                                                           |                          |                |                                                                                                                                                                                      |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   | Role                                                                                        |                          | code           |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | DatasetRole                                                                             |                          | code           | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty/gmd:role/gmd:CI_RoleCode                                             |               |       |        |                                           |       |
  |   |   |   | type                                                                                | string required          |                | gmd:role/gmd:CI_RoleCode/@codeSpace                                                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | uri                                                                                 | url required             |                | gmd:role/gmd:CI_RoleCode/@codeList                                                                                                                                                   |               |       | open   |                                           |       |
  |   |   |   | code                                                                                | string required          |                | gmd:role/gmd:CI_RoleCode/@codeListValue                                                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | name                                                                                | string required          |                | gmd:role/gmd:CI_RoleCode/#text                                                                                                                                                       |               |       | open   |                                           |       |
  |   | /                                                                                           |                          |                |                                                                                                                                                                                      |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Dataset                                                                                 |                          |                | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification                                                                                                                 |               |       |        |                                           |       |
  |   |   |   | keywords[]                                                                          | ref                      | Keyword        | gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString                                                                                                              |               |       | open   |                                           |       |
  |   |   |   | citation_title_xsi_type                                                             | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:title/@xsi:type                                                                                                                                     |               |       | open   |                                           |       |
  |   |   |   | citation_title_gco_character_string                                                 | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString                                                                                                                           |               |       | open   |                                           |       |
  |   |   |   | citation_title_pt_free_text_text_group_localised_character_string_locale            | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:title/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/@locale                                                                            |               |       | open   |                                           |       |
  |   |   |   | citation_title_pt_free_text_text_group_localised_character_string_text              | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:title/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/#text                                                                              |               |       | open   |                                           |       |
  |   |   |   | citation_date_ci_date_date_gco_date                                                 | date required unique     |                | gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | citation_date_ci_date_date_type_ci_date_type_code_code_space                        | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/@codeSpace                                                                                        |               |       | open   |                                           |       |
  |   |   |   | citation_date_ci_date_date_type_ci_date_type_code_code_list                         | url required unique      |                | gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/@codeList                                                                                         |               |       | open   |                                           |       |
  |   |   |   | citation_date_ci_date_date_type_ci_date_type_code_code_list_value                   | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/@codeListValue                                                                                    |               |       | open   |                                           |       |
  |   |   |   | citation_date_ci_date_date_type_ci_date_type_code_text                              | string required unique   |                | gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/#text                                                                                             |               |       | open   |                                           |       |
  |   |   |   | abstract_xsi_type                                                                   | string required unique   |                | gmd:abstract/@xsi:type                                                                                                                                                               |               |       | open   |                                           |       |
  |   |   |   | abstract_gco_character_string                                                       | string required unique   |                | gmd:abstract/gco:CharacterString                                                                                                                                                     |               |       | open   |                                           |       |
  |   |   |   | abstract_pt_free_text_text_group_localised_character_string_locale                  | string required unique   |                | gmd:abstract/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/@locale                                                                                                      |               |       | open   |                                           |       |
  |   |   |   | abstract_pt_free_text_text_group_localised_character_string_text                    | string required unique   |                | gmd:abstract/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/#text                                                                                                        |               |       | open   |                                           |       |
  |   |   |   | frequency_code_space                                                                | string required unique   |                | gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeSpace                                                   |               |       | open   |                                           |       |
  |   |   |   | frequency_code_list                                                                 | url required unique      |                | gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeList                                                    |               |       | open   |                                           |       |
  |   |   |   | frequency_code_list_value                                                           | string required unique   |                | gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeListValue                                               |               |       | open   |                                           |       |
  |   |   |   | frequency_code_text                                                                 | string required unique   |                | gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/#text                                                        |               |       | open   |                                           |       |
  |   |   |   | srv_service_type_gco_local_name                                                     | string required unique   |                | srv:serviceType/gco:LocalName                                                                                                                                                        |               |       | open   |                                           |       |
  |   |   |   | srv_service_type_version_gco_character_string                                       | string required unique   |                | srv:serviceTypeVersion/gco:CharacterString                                                                                                                                           |               |       | open   |                                           |       |
  |   |   |   | west_bound_longitude_gco_decimal                                                    | number required unique   |                | srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal                                                                       |               |       | open   |                                           |       |
  |   |   |   | east_bound_longitude_gco_decimal                                                    | number required unique   |                | srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal                                                                       |               |       | open   |                                           |       |
  |   |   |   | south_bound_latitude_gco_decimal                                                    | number required unique   |                | srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal                                                                       |               |       | open   |                                           |       |
  |   |   |   | north_bound_latitude_gco_decimal                                                    | number required unique   |                | srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal                                                                       |               |       | open   |                                           |       |
  |   |   |   | coupling_type_code_space                                                            | string required unique   |                | srv:couplingType/srv:SV_CouplingType/@codeSpace                                                                                                                                      |               |       | open   |                                           |       |
  |   |   |   | coupling_type_code_list                                                             | url required unique      |                | srv:couplingType/srv:SV_CouplingType/@codeList                                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | coupling_type_code_list_value                                                       | string required unique   |                | srv:couplingType/srv:SV_CouplingType/@codeListValue                                                                                                                                  |               |       | open   |                                           |       |
  |   |   |   | coupling_type_text                                                                  | string required unique   |                | srv:couplingType/srv:SV_CouplingType/#text                                                                                                                                           |               |       | open   |                                           |       |
  |   |   |   | operation_name_xsi_type                                                             | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:operationName/@xsi:type                                                                                                          |               |       | open   |                                           |       |
  |   |   |   | operation_name_gco_character_string                                                 | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:operationName/gco:CharacterString                                                                                                |               |       | open   |                                           |       |
  |   |   |   | operation_name_pt_free_text_text_group_localised_character_string_locale            | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:operationName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/@locale                                                 |               |       | open   |                                           |       |
  |   |   |   | operation_name_pt_free_text_text_group_localised_character_string_text              | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:operationName/gmd:PT_FreeText/gmd:textGroup/gmd:LocalisedCharacterString/#text                                                   |               |       | open   |                                           |       |
  |   |   |   | dcp_srv_dcp_list_code_space                                                         | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:DCP/srv:DCPList/@codeSpace                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | dcp_srv_dcp_list_code_list                                                          | url required unique      |                | srv:containsOperations/srv:SV_OperationMetadata/srv:DCP/srv:DCPList/@codeList                                                                                                        |               |       | open   |                                           |       |
  |   |   |   | dcp_srv_dcp_list_code_list_value                                                    | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:DCP/srv:DCPList/@codeListValue                                                                                                   |               |       | open   |                                           |       |
  |   |   |   | dcp_srv_dcp_list_text                                                               | string required unique   |                | srv:containsOperations/srv:SV_OperationMetadata/srv:DCP/srv:DCPList/#text                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | connect_point_ci_online_resource_linkage_url                                        | url required unique      |                | srv:containsOperations/srv:SV_OperationMetadata/srv:connectPoint/gmd:CI_OnlineResource/gmd:linkage/gmd:URL                                                                           |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Distribution                                                                            | download_url             |                | /gmd:MD_Metadata/gmd:distributionInfo/gmd:MD_Distribution                                                                                                                            |               |       |        |                                           |       |
  |   |   |   | format                                                                              | ref                      | Format         | gmd:distributionFormat/gmd:MD_Format/gmd:name/gco:CharacterString                                                                                                                    |               |       | open   |                                           |       |
  |   |   |   | download_url                                                                        | url required             |                | gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL                                                                               |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Format                                                                                  | title                    |                | /gmd:MD_Metadata/gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/gmd:MD_Format                                                                                       |               |       |        |                                           |       |
  |   |   |   | title                                                                               | string required          |                | gmd:name/gco:CharacterString                                                                                                                                                         |               |       | open   |                                           |       |
  |   |   |   | version                                                                             | string                   |                | gmd:version/gco:CharacterString                                                                                                                                                      |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | DescriptiveKeywords                                                                     |                          |                | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords                                                                                         |               |       |        |                                           |       |
  |   |   |   | keyword_gco_character_string                                                        | string unique            |                | gmd:MD_Keywords/gmd:keyword/gco:CharacterString                                                                                                                                      |               |       | open   |                                           |       |
  |   |   |   | thesaurus_name_ci_citation_title_gco_character_string                               | string unique            |                | gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString                                                                                                      |               |       | open   |                                           |       |
  |   |   |   | thesaurus_name_ci_citation_date_ci_date_date_gco_date                               | string unique            |                | gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date                                                                                             |               |       | open   |                                           |       |
  |   |   |   | thesaurus_name_ci_citation_date_ci_date_date_type_ci_date_type_code_code_list       | url                      |                | gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/@codeList                                                                    |               |       | open   |                                           |       |
  |   |   |   | thesaurus_name_ci_citation_date_ci_date_date_type_ci_date_type_code_code_list_value | string                   |                | gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/@codeListValue                                                               |               |       | open   |                                           |       |
  |   |   |   | thesaurus_name_ci_citation_date_ci_date_date_type_ci_date_type_code_text            | string                   |                | gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode/#text                                                                        |               |       | open   |                                           |       |
  |   |   |   | type_md_keyword_type_code_code_space                                                | string unique            |                | gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode/@codeSpace                                                                                                                           |               |       | open   |                                           |       |
  |   |   |   | type_md_keyword_type_code_code_list                                                 | url unique               |                | gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode/@codeList                                                                                                                            |               |       | open   |                                           |       |
  |   |   |   | type_md_keyword_type_code_code_list_value                                           | string unique            |                | gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode/@codeListValue                                                                                                                       |               |       | open   |                                           |       |
  |   |   |   | type_md_keyword_type_code_text                                                      | string unique            |                | gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode/#text                                                                                                                                |               |       | open   |                                           |       |
  |   |   |   | parent                                                                              | ref                      | MetaData       | ../../../..                                                                                                                                                                          |               |       | open   |                                           |       |
                                                                                                    |                          |                |                                                                                                                                                                                      |               |       |        |                                           |       |
  |   |   | Keyword                                                                                 | name                     |                | /gmd:MD_Metadata/gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword                                                             |               |       |        |                                           |       |
  |   |   |   | name                                                                                | string required          |                | gco:CharacterString                                                                                                                                                                  |               |       | open   |                                           |       |
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

http GET "$SERVER/$DATASET/Page?format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FileNotFoundError",
#|             "message": "[Errno 2] No such file or directory: '{url}'"
#|         }
#|     ]
#| }
tail -200 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 162, in create_http_response
#|     return await commands.getall(
#|            ^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/read.py", line 125, in getall
#|     return render(context, request, model, params, rows, action=action)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/formats/ascii/commands.py", line 28, in render
#|     return _render(
#|            ^^^^^^^^
#|   File "spinta/formats/ascii/commands.py", line 62, in _render
#|     aiter(peek_and_stream(fmt(
#|           ^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 232, in peek_and_stream
#|     peek = list(itertools.islice(stream, 2))
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/formats/ascii/components.py", line 41, in __call__
#|     peek = next(data, None)
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/accesslog/__init__.py", line 162, in log_response
#|     for row in rows:
#|   File "spinta/commands/read.py", line 110, in <genexpr>
#|     rows = (
#|            ^
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 371, in getall
#|     yield from _dask_get_all(context, query, df, backend, model, builder)
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 436, in _dask_get_all
#|     for i, row in qry.iterrows():
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 310, in _get_data_json
#|     with pathlib.Path(url).open(encoding='utf-8-sig') as f:
#|          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "/usr/lib/python3.11/pathlib.py", line 1044, in open
#|     return io.open(self, mode, buffering, encoding, errors, newline)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#| FileNotFoundError: [Errno 2] No such file or directory: '{url}'
# FIXME: Parametrization does not work, because `{url}` must be replaced by
#        page url as described with `url` param.


http GET "$SERVER/$DATASET/DatasetOrganization?format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "XMLSyntaxError",
#|             "message": "Document is empty, line 1, column 1 (<string>, line 1)"
#|         }
#|     ]
#| }
tail -200 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 162, in create_http_response
#|     return await commands.getall(
#|            ^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/read.py", line 125, in getall
#|     return render(context, request, model, params, rows, action=action)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/formats/ascii/commands.py", line 28, in render
#|     return _render(
#|            ^^^^^^^^
#|   File "spinta/formats/ascii/commands.py", line 62, in _render
#|     aiter(peek_and_stream(fmt(
#|           ^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 232, in peek_and_stream
#|     peek = list(itertools.islice(stream, 2))
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/formats/ascii/components.py", line 41, in __call__
#|     peek = next(data, None)
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/accesslog/__init__.py", line 162, in log_response
#|     for row in rows:
#|   File "spinta/commands/read.py", line 110, in <genexpr>
#|     rows = (
#|            ^
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 299, in _get_data_xml
#|     return _parse_xml(io.BytesIO(f.content), source, model_props)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 139, in _parse_xml
#|     for action, elem in etree.iterparse(data, events=('start', 'end'), remove_blank_text=True):
#|   File "src/lxml/iterparse.pxi", line 210, in lxml.etree.iterparse.__next__
#|   File "src/lxml/iterparse.pxi", line 195, in lxml.etree.iterparse.__next__
#|   File "src/lxml/iterparse.pxi", line 230, in lxml.etree.iterparse._read_more_events
#|   File "src/lxml/parser.pxi", line 1379, in lxml.etree._FeedParser.feed
#|   File "src/lxml/parser.pxi", line 609, in lxml.etree._ParserContext._handleParseResult
#|   File "src/lxml/parser.pxi", line 618, in lxml.etree._ParserContext._handleParseResultDoc
#|   File "src/lxml/parser.pxi", line 728, in lxml.etree._handleParseResult
#|   File "src/lxml/parser.pxi", line 657, in lxml.etree._raiseParseError
#|   File "<string>", line 1
#| lxml.etree.XMLSyntaxError: Document is empty, line 1, column 1
# FIXME: Probably issue with the pagination
