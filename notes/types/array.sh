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

psql -h localhost -p 54321 -U admin spinta -c 'SELECT * FROM "'$DATASET'/Country/:list/languages";'
#|                  _txn                 |                 _rid                 | languages._id 
#| --------------------------------------+--------------------------------------+---------------
#|  d16de594-ba6a-485d-b411-7289ff286757 | d73306fb-4ee5-483d-9bad-d86f98e1869c | 


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
#|             "name": "Lithuania"
# FIXME: Here I expect to see:
#        "languages": []
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
#|             "name": "Lithuania"
# FIXME: Here I expect to see:
#        "languages": [
#            {"_id": "$LIT"},
#            {"_id": "$SGS"}
#        ]
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Country?expand(languages)"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "component": "spinta.components.Model",
#|                 "dataset": "types/array",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/array/Country",
#|                 "property": "languages",
#|                 "schema": "7"
#|             },
#|             "message": "Unknown property 'languages'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         }
#|     ]
#| }
# TODO: `expand(languages)` should work.


http GET "$SERVER/$DATASET/Country?format(ascii)"
#| name     
#| ---------
#| Lithuania
# FIXME: `languages[]` sould be shown here.


http GET "$SERVER/$DATASET/Country?expand()&format(rdf)"
#| <?xml version="1.0" encoding="UTF-8"?>
#| <Rdf xml:base="http://localhost:8000/">
#|   <Description about="/types/array/Country/d73306fb-4ee5-483d-9bad-d86f98e1869c" type="types/array/Country" version="7ac9981c-2158-4ac9-8f8e-7849ee194ec8">
#|     <_page>WyJkNzMzMDZmYi00ZWU1LTQ4M2QtOWJhZC1kODZmOThlMTg2OWMiXQ==</_page>
#|     <name>Lithuania</name>
# FIXME: Here I expect to see:
#        <languages rdf:resource="/types/array/Language/$LIT" />
#        <languages rdf:resource="/types/array/Language/$SGS" />
#|   </Description>
#| </Rdf>


http GET "$SERVER/$DATASET/Country/:changes"
#| {
#|     "_data": [
#|         {
#|             "_cid": 1,
#|             "_created": "2023-10-04T12:06:16.907090",
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_op": "insert",
#|             "_revision": "fa4dd99f-a782-4d13-9480-0a258a9d0790",
#|             "_txn": "95a21aec-cd77-42fa-b0f1-dde4c00e6292",
#|             "name": "Lithuania"
# FIXME: Languages should be shown here:
#          "languages": [
#              {"_id": "$LIT"}
#          ]
#|         },
#|         {
#|             "_cid": 2,
#|             "_created": "2023-10-04T12:12:08.114911",
#|             "_id": "d73306fb-4ee5-483d-9bad-d86f98e1869c",
#|             "_op": "patch",
#|             "_revision": "7ac9981c-2158-4ac9-8f8e-7849ee194ec8",
#|             "_txn": "bc01d1d0-8611-4f45-a1e7-6665a10b26ff"
# FIXME: Languages should be shown here:
#          "languages": [
#              {"_id": "$LIT"},
#              {"_id": "$SGS"}
#          ]
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



