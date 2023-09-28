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

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
#|                     List of relations
#|  Schema |              Name               | Type  | Owner 
#| --------+---------------------------------+-------+-------
#|  public | types/array/Country             | table | admin
#|  public | types/array/Country/:changelog  | table | admin
#|  public | types/array/Language            | table | admin
#|  public | types/array/Language/:changelog | table | admin
# FIXME: for some reason there si no Language/:list/languages table

# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Language?format(ascii)"
#| HTTP/1.1 200 OK

LTL=c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0
http POST "$SERVER/$DATASET/Language" $AUTH _id=$LTL name=Lithuanian
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "c8e4cd60-0b15-4b23-a691-09cdf2ebd9c0",
#|     "_revision": "4a825ed1-0936-4668-ab80-c3bd424ec3ef",
#|     "_type": "types/array/Language",
#|     "countries[]": null,
#|     "name": "Lithuanian"
#| }
REV=$(http GET "$SERVER/$DATASET/Language/$LTL" | jq -r '._revision')


LTC=d73306fb-4ee5-483d-9bad-d86f98e1869c
http POST "$SERVER/$DATASET/Country" $AUTH <<EOF
{
    "_id": "$LTC",
    "name": "Lithuania",
    "languages": [
        {"_id": "$LTL"}
    ]
}
EOF
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
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property 'languages'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "model"
#|         }
#|     ]
#| }
# FIXME: This should work.
tail -80 $BASEDIR/spinta.log
#| ERROR: Error: Multiple errors:
#|  - Unknown property 'languages'.
#|      Context:
#|        component: spinta.components.Model
#|        manifest: default
#|        schema: 7
#|        dataset: types/array
#|        model: types/array/Country
#|        entity: 
#|        property: languages
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 210, in create_http_response
#|     return await commands.push(
#|            ^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/write.py", line 102, in push
#|     status_code, response = await simple_response(
#|                             ^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/write.py", line 1184, in simple_response
#|     results = await alist(aslice(results, 2))
#|               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/aiotools.py", line 51, in alist
#|     return [x async for x in it]
#|            ^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/aiotools.py", line 51, in <listcomp>
#|     return [x async for x in it]
#|            ^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/aiotools.py", line 62, in aslice
#|     async for x in it:
#|   File "spinta/commands/write.py", line 187, in push_stream
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/changes.py", line 25, in create_changelog_entry
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/write.py", line 31, in insert
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 861, in prepare_data_for_write
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 648, in prepare_patch
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 618, in validate_data
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 521, in read_existing_data
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 509, in prepare_data
#|     data.given = commands.load(context, data.model, data.payload)
#|                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/types/model.py", line 370, in load
#|     raise exceptions.MultipleErrors(
#| spinta.exceptions.MultipleErrors: Multiple errors:
#|  - Unknown property 'languages'.
#|      Context:
#|        component: spinta.components.Model
#|        manifest: default
#|        schema: 7
#|        dataset: types/array
#|        model: types/array/Country
#|        entity: 
#|        property: languages
