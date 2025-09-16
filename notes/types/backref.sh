# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/backref
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref      | access
$DATASET                 |         |          |
                         |         |          |
  |   |   | Country      |         |          |
  |   |   |   | name     | string  |          | open
  |   |   |   | cities[] | backref | City     | open
                         |         |          |
  |   |   | City         |         |          |
  |   |   |   | name     | string  |          | open
  |   |   |   | country  | ref     | Country  | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
#|                      List of relations
#|  Schema |               Name               | Type  | Owner 
#| --------+----------------------------------+-------+-------
#|  public | types/backref/City               | table | admin
#|  public | types/backref/City/:changelog    | table | admin
#|  public | types/backref/Country            | table | admin
#|  public | types/backref/Country/:changelog | table | admin

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|                    Table "public.types/backref/Country"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _txn      | uuid                        |           |          | 
#|  _created  | timestamp without time zone |           |          | 
#|  _updated  | timestamp without time zone |           |          | 
#|  _id       | uuid                        |           | not null | 
#|  _revision | text                        |           |          | 
#|  name      | text                        |           |          | 
#| Indexes:
#|     "types/backref/Country_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/backref/Country__txn" btree (_txn)
#| Referenced by:
#|     TABLE ""types/backref/City""
#|         CONSTRAINT "fk_types/backref/City_country._id"
#|         FOREIGN KEY ("country._id")
#|         REFERENCES "types/backref/Country"(_id)

# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| HTTP/1.1 200 OK

http GET "$SERVER/$DATASET/City?format(ascii)"
#| HTTP/1.1 200 OK

LT=d73306fb-4ee5-483d-9bad-d86f98e1869c
http POST "$SERVER/$DATASET/Country" $AUTH <<EOF
{
    "_id": "$LT",
    "name": "Lithuania"
}
EOF
#| HTTP/1.1 201 Created

VLN=328cfde7-2365-4625-bf72-e9b3038e12ac
http POST "$SERVER/$DATASET/City" $AUTH <<EOF
{
    "_id": "$VLN",
    "name": "Vilnius",
    "country": {"_id": "$LT"}
}
EOF

KNS=77424289-2e18-4f9b-8446-644b5da4bd1a
http POST "$SERVER/$DATASET/City" $AUTH <<EOF
{
    "_id": "$KNS",
    "name": "Kaunas",
    "country": {"_id": "$LT"}
}
EOF

http GET "$SERVER/$DATASET/Country"
#| {
#|     "_data": [
#|         {
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_page": "WyJkNzMzMDZmYi00ZWU1LTQ4M2QtOWJhZC1kODZmOThlMTg2OWMiXQ==",
#|             "_revision": "31ce7684-f27b-4be1-ae11-1c4e7950d7a2",
#|             "_type": "types/backref/Country",
#|             "name": "Lithuania",
#|             "cities": []
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Country?expand()"
#| {
#|     "_data": [
#|         {
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_page": "WyJkNzMzMDZmYi00ZWU1LTQ4M2QtOWJhZC1kODZmOThlMTg2OWMiXQ==",
#|             "_revision": "31ce7684-f27b-4be1-ae11-1c4e7950d7a2",
#|             "_type": "types/backref/Country",
#|             "name": "Lithuania",
#|             "cities": [
#|                 {"_id": "328cfde7-2365-4625-bf72-e9b3038e12ac"},
#|                 {"_id": "77424289-2e18-4f9b-8446-644b5da4bd1a"}
#|             ]
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/City"
#| {
#|     "_data": [
#|         {
#|             "_id": "328cfde7-2365-4625-bf72-e9b3038e12ac",
#|             "_page": "WyIzMjhjZmRlNy0yMzY1LTQ2MjUtYmY3Mi1lOWIzMDM4ZTEyYWMiXQ==",
#|             "_revision": "14d3d913-4daa-4b4d-b5cb-2dcd282124f0",
#|             "_type": "types/backref/City",
#|             "name": "Vilnius"
#|             "country": {"_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c"},
#|         },
#|         {
#|             "_id": "77424289-2e18-4f9b-8446-644b5da4bd1a",
#|             "_page": "WyI3NzQyNDI4OS0yZTE4LTRmOWItODQ0Ni02NDRiNWRhNGJkMWEiXQ==",
#|             "_revision": "07b56050-dd87-42b2-8e5c-74b2c410213d",
#|             "_type": "types/backref/City",
#|             "name": "Kaunas"
#|             "country": {"_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c"},
#|         }
#|     ]
#| }


http GET "$SERVER/$DATASET/Country?select(_id,name,cities)&format(ascii)"
#| _id                                   name       cities[]._id
#| ------------------------------------  ---------  ------------
#| d73306fb-4ee5-483d-9bad-d86f98e1869c  Lithuania  ∅

http GET "$SERVER/$DATASET/Country?select(_id,name,cities)&expand()&format(ascii)"
#| _id                                   name       cities[]._id                        
#| ------------------------------------  ---------  ------------------------------------
#| d73306fb-4ee5-483d-9bad-d86f98e1869c  Lithuania  328cfde7-2365-4625-bf72-e9b3038e12ac
#| d73306fb-4ee5-483d-9bad-d86f98e1869c  Lithuania  77424289-2e18-4f9b-8446-644b5da4bd1a


http GET "$SERVER/$DATASET/Country?select(name,cities)&format(ascii)"
#| name       cities[]._id
#| ---------  ------------
#| Lithuania  ∅
# TODO: I guess `cities` should be expanded if requested explicity in
#       `select()`?


http GET "$SERVER/$DATASET/Country?select(name,cities[].name)&format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "AttributeError",
#|             "message": "'Visitor' object has no attribute 'trailer_comp'"
#|         }
#|     ]
#| }
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 86, in homepage
#|     params: UrlParams = prepare(context, UrlParams(), Version(), request)
#|   File "spinta/urlparams.py", line 37, in prepare
#|     parse_url_query(urllib.parse.unquote(request.url.query))
#|   File "spinta/urlparams.py", line 79, in parse_url_query
#|     rql = spyna.parse(query)
#|   File "spinta/spyna.py", line 88, in parse
#|     return visit(ast)
#|   File "spinta/spyna.py", line 97, in __call__
#|     return getattr(self, node.data, self.__default__)(node, *node.children)
#|   File "spinta/spyna.py", line 108, in __default__
#|     'args': self._args(*args),
#|   File "spinta/spyna.py", line 121, in _args
#|     return [self(arg) for arg in args]
#|   File "spinta/spyna.py", line 121, in <listcomp>
#|     return [self(arg) for arg in args]
#|             ^^^^^^^^^
#|   File "spinta/spyna.py", line 97, in __call__
#|     return getattr(self, node.data, self.__default__)(node, *node.children)
#|   File "spinta/spyna.py", line 192, in func
#|     'args': self._args(*args.children),
#|   File "spinta/spyna.py", line 121, in _args
#|     return [self(arg) for arg in args]
#|   File "spinta/spyna.py", line 121, in <listcomp>
#|     return [self(arg) for arg in args]
#|             ^^^^^^^^^
#|   File "spinta/spyna.py", line 97, in __call__
#|     return getattr(self, node.data, self.__default__)(node, *node.children)
#|   File "spinta/spyna.py", line 220, in composition
#|     handler = getattr(self, arg.data + '_comp')
#| AttributeError: 'Visitor' object has no attribute 'trailer_comp'
# TODO: Add support for select(cities[].name) make sure this works with backref
#       type.


http GET "$SERVER/$DATASET/Country?cities[].name='Kaunas'"
#| HTTP/1.1 500 Internal Server Error
# TODO: Same as above. This should select Countries with cities named "Kaunas".

http GET "$SERVER/$DATASET/Country?cities[name='Kaunas']"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "UnknownMethod",
#|             "context": {
#|                 "expr": "filter(cities, [{'type': 'expression', 'name': 'eq', 'args': [{'name': 'bind', 'args': ['name']}, 'Kaunas']}])",
#|                 "name": "filter"
#|             },
#|             "message": "Unknown method 'filter' with args filter(cities, [{'type': 'expression', 'name': 'eq', 'args': [{'name': 'bind', 'args': ['name']}, 'Kaunas']}]).",
#|             "template": "Unknown method {name!r} with args {expr}.",
#|             "type": "system"
#|         }
#|     ]
#| }
# TODO: This should select all Countries, but Country.cities should only
#       contain cities named "Kaunas".


http -b GET "$SERVER/$DATASET/Country?format(csv)" | csvlook
#| | _id                                  | name      | cities[]._id |
#| | ------------------------------------ | --------- | ------------ |
#| | d73306fb-4ee5-483d-9bad-d86f98e1869c | Lithuania |              |

http -b GET "$SERVER/$DATASET/Country?expand()&format(csv)" | csvlook
#| | _id                                  | name      | cities[]._id                         |
#| | ------------------------------------ | --------- | ------------------------------------ |
#| | d73306fb-4ee5-483d-9bad-d86f98e1869c | Lithuania | 328cfde7-2365-4625-bf72-e9b3038e12ac |
#| | d73306fb-4ee5-483d-9bad-d86f98e1869c | Lithuania | 77424289-2e18-4f9b-8446-644b5da4bd1a |


http GET "$SERVER/$DATASET/Country?format(rdf)"
#| HTTP/1.1 200 OK
#| http: LogLevel.ERROR: ChunkedEncodingError: ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)", InvalidChunkLength(got length b'', 0 bytes read))
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/formats/rdf/commands.py", line 218, in _stream
#|     yield _prepare_for_print(model, row)
#|   File "spinta/formats/rdf/commands.py", line 160, in _prepare_for_print
#|     elem = _create_model_element(model, data)
#|   File "spinta/formats/rdf/commands.py", line 148, in _create_model_element
#|     return _create_element(
#|   File "spinta/formats/rdf/commands.py", line 119, in _create_element
#|     elem.append(child)
#| TypeError: Argument 'element' has incorrect type (expected lxml.etree._Element, got list)
# TODO: Peek data, before starting response, in order to avoid 200 OK response
#       with an error.
# TODO: Add RDF support


http GET "$SERVER/$DATASET/Country?expand()&format(rdf)"
#| HTTP/1.1 200 OK
# TODO: Same as above.


http DELETE "$SERVER/$DATASET/:wipe" $AUTH
#| {
#|     "wiped": true
#| }

http GET "$SERVER/$DATASET/Country"
http GET "$SERVER/$DATASET/City"

