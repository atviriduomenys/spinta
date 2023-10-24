 # 2023-02-16 17:11

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/array
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref      | access
$DATASET                    |         |          |
                            |         |          |
  |   |   | Language        |         |          |
  |   |   |   | name        | string  |          | open
                            |         |          |
  |   |   | Country         |         |          |
  |   |   |   | name        | string  |          | open
  |   |   |   | languages[] | ref     | Language | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
#|                       List of relations
#|  Schema |                Name                 | Type  | Owner 
#| --------+-------------------------------------+-------+-------
#|  public | types/array/Country                 | table | admin
#|  public | types/array/Country/:list/languages | table | admin
#|  public | types/array/Language                | table | admin

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country/:list/languages"'
#| Table "public.types/array/Country/:list/languages"
#|  Column | Type | Collation | Nullable | Default 
#| --------+------+-----------+----------+---------
#|  _txn   | uuid |           |          | 
#|  _rid   | uuid |           |          | 
#|  _id    | uuid |           |          | 
#| Indexes:
#|     "ix_types/array/Country/:list/languages__txn" btree (_txn)
#| Foreign-key constraints:
#|     "fk_types/array/Country__id"
#|         FOREIGN KEY (_id)
#|             REFERENCES "types/array/Language"(_id)
#|                 ON UPDATE CASCADE ON DELETE CASCADE
#|     "types/array/Country/:list/languages__rid_fkey"
#|         FOREIGN KEY (_rid)
#|             REFERENCES "types/array/Country"(_id)
#|                 ON DELETE CASCADE

# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Language?format(ascii)"
#| HTTP/1.1 200 OK

LIT=c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0
http POST "$SERVER/$DATASET/Language" $AUTH _id=$LIT name=Lithuanian
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0",
#|     "_revision": "bc3c4024-fb94-47de-beeb-1b11111b39d9",
#|     "_type": "types/array/Language",
#|     "name": "Lithuanian"
#| }


LT=d73306fb-4ee5-483d-9bad-d86f98e1869c
http POST "$SERVER/$DATASET/Country" $AUTH <<EOF
{
    "_id": "$LT",
    "name": "Lithuania",
    "languages": [
        {"_id": "$LIT"}
    ]
}
EOF
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|     "_revision": "fa4dd99f-a782-4d13-9480-0a258a9d0790",
#|     "_type": "types/array/Country",
#|     "languages": [
#|         {
#|             "_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
#|         }
#|     ],
#|     "name": "Lithuania"
#| }

psql -h localhost -p 54321 -U admin spinta -c 'SELECT * FROM "'$DATASET'/Country";'
#|                  _txn                 |          _created          | _updated |                 _id                  |              _revision               |   name    |                     languages                     
#| --------------------------------------+----------------------------+----------+--------------------------------------+--------------------------------------+-----------+---------------------------------------------------
#|  a8072b1c-3261-46a6-9e3c-6425e18345cd | 2023-10-06 04:11:36.217247 |          | d73306fb-4ee5-483d-9bad-d86f98e1869c | 545e40c6-0be4-4a29-9c7b-4ac4fb2ae256 | Lithuania | [{"_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"}]

psql -h localhost -p 54321 -U admin spinta -c 'SELECT * FROM "'$DATASET'/Country/:list/languages";'
#|                  _txn                 |                 _rid                 |                 _id                  
#| --------------------------------------+--------------------------------------+--------------------------------------
#|  f97f5e0d-9122-4974-ac5b-6316f2fb0da3 | d73306fb-4ee5-483d-9bad-d86f98e1869c | c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0


SGS=328cfde7-2365-4625-bf72-e9b3038e12ac
http POST "$SERVER/$DATASET/Language" $AUTH _id=$SGS name=Samogitian

REV=$(http GET "$SERVER/$DATASET/Country/$LT" | jq -r '._revision')
http PATCH "$SERVER/$DATASET/Country/$LT" $AUTH <<EOF
{
    "_revision": "$REV",
    "languages": [
        {"_id": "$LIT"},
        {"_id": "$SGS"}
    ]
}
EOF
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|     "_revision": "7ac9981c-2158-4ac9-8f8e-7849ee194ec8",
#|     "_type": "types/array/Country",
#|     "languages": [
#|         {
#|             "_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"
#|         },
#|         {
#|             "_id": "328cfde7-2365-4625-bf72-e9b3038e12ac"
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Country"
#| {
#|     "_data": [
#|         {
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_page": "WyJkNzMzMDZmYi00ZWU1LTQ4M2QtOWJhZC1kODZmOThlMTg2OWMiXQ==",
#|             "_revision": "7ac9981c-2158-4ac9-8f8e-7849ee194ec8",
#|             "_type": "types/array/Country",
#|             "languages": [],
#|             "name": "Lithuania"
#|         }
#|     ]
#| }


http GET "$SERVER/$DATASET/Country/languages"
#| HTTP/1.1 404 Not Found
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "ModelNotFound",
#|             "context": {
#|                 "model": "types/array/Country/languages"
#|             },
#|             "message": "Model 'types/array/Country/languages' not found.",
#|             "template": "Model {model!r} not found.",
#|             "type": "system"
#|         }
#|     ]
#| }
# TODO: This should work.


http GET "$SERVER/$DATASET/Country/$LT/languages"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "UnavailableSubresource",
#|             "context": {
#|                 "prop": "languages",
#|                 "prop_type": "array"
#|             },
#|             "message": "Subresources only of type Object and File are accessible",
#|             "template": "Subresources only of type Object and File are accessible",
#|             "type": "system"
#|         }
#|     ]
#| }
# TODO: This should work.


http GET "$SERVER/$DATASET/Country?expand()"
#| {
#|     "_data": [
#|         {
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_page": "WyJkNzMzMDZmYi00ZWU1LTQ4M2QtOWJhZC1kODZmOThlMTg2OWMiXQ==",
#|             "_revision": "7ac9981c-2158-4ac9-8f8e-7849ee194ec8",
#|             "_type": "types/array/Country",
#|             "languages": [
#|                 {"_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"},
#|                 {"_id": "328cfde7-2365-4625-bf72-e9b3038e12ac"}
#|             ],
#|             "name": "Lithuania"
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Country?expand(languages)"
#| {
#|     "_data": [
#|         {
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_page": "WyJkNzMzMDZmYi00ZWU1LTQ4M2QtOWJhZC1kODZmOThlMTg2OWMiXQ==",
#|             "_revision": "0433647b-54f4-4266-a1f9-99fab75dec9c",
#|             "_type": "types/array/Country",
#|             "languages": [
#|                 {"_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"},
#|                 {"_id": "328cfde7-2365-4625-bf72-e9b3038e12ac"}
#|             ],
#|             "name": "Lithuania"
#|         }
#|     ]
#| }


http GET "$SERVER/$DATASET/Country?format(ascii)"
#| name       languages[]._id
#| ---------  ---------------
#| Lithuania  ∅


http GET "$SERVER/$DATASET/Country?expand()&format(rdf)"
#| HTTP/1.1 200 OK
#| 
#| http: LogLevel.ERROR: ChunkedEncodingError: ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)", InvalidChunkLength(got length b'', 0 bytes read))
# FIXME: RDF-XML export does not work.
tail -50 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/formats/rdf/commands.py", line 218, in _stream
#|     yield _prepare_for_print(model, row)
#|           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/formats/rdf/commands.py", line 160, in _prepare_for_print
#|     elem = _create_model_element(model, data)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/formats/rdf/commands.py", line 148, in _create_model_element
#|     return _create_element(
#|            ^^^^^^^^^^^^^^^^
#|   File "spinta/formats/rdf/commands.py", line 119, in _create_element
#|     elem.append(child)
#| TypeError: Argument 'element' has incorrect type (expected lxml.etree._Element, got list)
# FIXME: Fix array support for RDF format.

http GET "$SERVER/$DATASET/Country/:changes"
#| {
#|     "_data": [
#|         {
#|             "_cid": 1,
#|             "_created": "2023-10-09T07:33:50.112187",
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_op": "insert",
#|             "_revision": "e3e3d736-143a-4d80-87f5-eb0fafb4280c",
#|             "_txn": "f97f5e0d-9122-4974-ac5b-6316f2fb0da3",
#|             "languages": [
#|                 {"_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"}
#|             ],
#|             "name": "Lithuania"
#|         },
#|         {
#|             "_cid": 2,
#|             "_created": "2023-10-09T07:35:29.477286",
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_op": "patch",
#|             "_revision": "0433647b-54f4-4266-a1f9-99fab75dec9c",
#|             "_txn": "25341cb7-0eb8-4611-9e00-2b4333df82bb",
#|             "languages": [
#|                 {"_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0"},
#|                 {"_id": "328cfde7-2365-4625-bf72-e9b3038e12ac"}
#|             ]
#|         }
#|     ]
#| }

http DELETE "$SERVER/$DATASET/Country/$LT" $AUTH
#| HTTP/1.1 204 No Content

http GET "$SERVER/$DATASET/Country"
#| {"_data": []}

http POST "$SERVER/$DATASET/Country" $AUTH <<EOF
{
    "_id": "$LT",
    "name": "Lithuania",
    "languages": [
        {"_id": "$LIT"},
        {"_id": "$SGS"}
    ]
}
EOF

http GET "$SERVER/$DATASET/Country?format(ascii)"

http DELETE "$SERVER/$DATASET/Country/:wipe" $AUTH
#| {"wiped": true}

http GET "$SERVER/$DATASET/Country?format(ascii)"


psql -h localhost -p 54321 -U admin spinta -c 'SELECT * FROM "'$DATASET'/Country/:list/languages";'
#|  _txn | _rid | languages._id 
#| ------+------+---------------
#| (0 rows)

http DELETE "$SERVER/$DATASET/:wipe" $AUTH
