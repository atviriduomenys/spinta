INSTANCE=datasets/xml
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

unset SPINTA_CONFIG

http -b get http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos | head
#| <?xml version="1.0" encoding="UTF-8"?>
#|     <SeimoInformacija
#|         pavadinimas="Seimo kadencijos"
#|         suformavimo_laikas="2023-07-26 11:44:45"
#|         šaltinis="www.lrs.lt"
#|         tiekėjas="Seimo kanceliarija"
#|         licencija="CC BY 4.0"
#|     >
#|         <SeimoKadencija
#|             kadencijos_id="1"
#|             pavadinimas="1990–1992 metų kadencija"
#|             data_nuo="1990-03-10"
#|             data_iki="1992-11-22"
#|         />

poetry run spinta inspect -r xml http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos
#| id | d | r | b | m | property                             | type                     | ref    | source                                         | prepare | level | access | uri | title | description
#|    | dataset                                              |                          |        |                                                |         |       |        |     |       |
#|    |   | resource                                         | xml                      |        | http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos |         |       |        |     |       |
#|    |                                                      |                          |        |                                                |         |       |        |     |       |
#|    |   |   |   | Model1                                   |                          |        | .                                              |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_pavadinimas        | string required unique   |        | SeimoInformacija/@pavadinimas                  |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_suformavimo_laikas | datetime required unique |        | SeimoInformacija/@suformavimo_laikas           |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_saltinis           | string required unique   |        | SeimoInformacija/@šaltinis                     |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_tiekejas           | string required unique   |        | SeimoInformacija/@tiekėjas                     |         |       |        |     |       |
#|    |   |   |   |   | seimo_informacija_licencija          | string required unique   |        | SeimoInformacija/@licencija                    |         |       |        |     |       |
#|    |                                                      |                          |        |                                                |         |       |        |     |       |
#|    |   |   |   | SeimoKadencija                           |                          |        | /SeimoInformacija/SeimoKadencija               |         |       |        |     |       |
#|    |   |   |   |   | kadencijos_id                        | integer required unique  |        | @kadencijos_id                                 |         |       |        |     |       |
#|    |   |   |   |   | pavadinimas                          | string required unique   |        | @pavadinimas                                   |         |       |        |     |       |
#|    |   |   |   |   | data_nuo                             | date required unique     |        | @data_nuo                                      |         |       |        |     |       |
#|    |   |   |   |   | data_iki                             | date unique              |        | @data_iki                                      |         |       |        |     |       |
#|    |   |   |   |   | parent                               | ref                      | Model1 | ../..                                          |         |       |        |     |       |

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property       | type                     | ref | source                                         | prepare | level | access
$DATASET                       |                          |     |                                                |         |       |
  | kadencijos                 | xml                      |     | http://apps.lrs.lt/sip/p2b.ad_seimo_kadencijos |         |       |
  |   |   | SeimoKadencija     |                          | id  | /SeimoInformacija/SeimoKadencija               |         | 4     |
  |   |   |   | id             | integer required unique  |     | @kadencijos_id                                 |         | 4     | open
  |   |   |   | pavadinimas    | string required          |     | @pavadinimas                                   |         | 4     | open
  |   |   |   | data_nuo       | date required            |     | @data_nuo                                      |         | 3     | open
  |   |   |   | data_iki       | date                     |     | @data_iki                                      |         | 3     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/SeimoKadencija"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "XMLSyntaxError",
#|             "message": "no element found (line 0)"
#|         }
#|     ]
#| }
tail -200 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 153, in create_http_response
#|     return await commands.getall(
#|   File "spinta/commands/read.py", line 125, in getall
#|     return render(context, request, model, params, rows, action=action)
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|   File "multipledispatch/dispatcher.py", line 278, in __call__
#|     return func(*args, **kwargs)
#|   File "spinta/formats/json/commands.py", line 25, in render
#|     return _render(
#|   File "spinta/formats/json/commands.py", line 52, in _render
#|     aiter(peek_and_stream(fmt(data))),
#|           ^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 223, in peek_and_stream
#|     peek = list(itertools.islice(stream, 2))
#|   File "spinta/formats/json/components.py", line 16, in __call__
#|     for i, row in enumerate(data):
#|   File "spinta/accesslog/__init__.py", line 162, in log_response
#|     for row in rows:
#|   File "spinta/commands/read.py", line 110, in <genexpr>
#|     rows = (
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 398, in getall
#|     yield from _dask_get_all(context, query, df, backend, model, builder)
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 435, in _dask_get_all
#|     for i, row in qry.iterrows():
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 298, in _get_data_xml
#|     return _parse_xml(f.raw, source, model_props)
#|   File "spinta/datasets/backends/dataframe/commands/read.py", line 138, in _parse_xml
#|     for action, elem in etree.iterparse(data, events=('start', 'end'), remove_blank_text=True):
#|   File "src/lxml/iterparse.pxi", line 210, in lxml.etree.iterparse.__next__
#|   File "src/lxml/iterparse.pxi", line 195, in lxml.etree.iterparse.__next__
#|   File "src/lxml/iterparse.pxi", line 226, in lxml.etree.iterparse._read_more_events
#|   File "src/lxml/parser.pxi", line 1395, in lxml.etree._FeedParser.close
#|   File "<string>", line 0
#| lxml.etree.XMLSyntaxError: no element found
