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
  |   |   |   | countries[] | backref | Country  | open
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
#|  Schema |                  Name                  | Type  | Owner 
#| --------+----------------------------------------+-------+-------
#|  public | types/array/Country                    | table | admin
#|  public | types/array/Country/:changelog         | table | admin
#|  public | types/array/Language                   | table | admin
#|  public | types/array/Language/:changelog        | table | admin
#|  public | types/array/Language/:list/countries[] | table | admin
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|      Column      |            Type             | Collation | Nullable | Default 
#| -----------------+-----------------------------+-----------+----------+---------
#|  name            | text                        |           |          | 
#|  languages[]._id | uuid                        |           |          | 
#| Foreign-key constraints:
#|     "fk_types/array/Country_languages[]._id"
#|         FOREIGN KEY ("languages[]._id")
#|         REFERENCES "types/array/Language"(_id)
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Language/:list/countries[]"'
#|           Column          | Type | Collation | Nullable | Default 
#| --------------------------+------+-----------+----------+---------
#|  _txn                     | uuid |           |          | 
# XXX: Probably _txn should not be here?
#|  types/array/Language._id | uuid |           |          | 
#|  countries[]._id          | uuid |           |          | 
#| Indexes:
#|     "ix_types/array/Language/:list/countries[]__txn" btree (_txn)
#| Foreign-key constraints:
#|     "types/array/Language/:list/countr_types/array/Language._id_fkey" FOREIGN KEY ("types/array/Language._id") REFERENCES "types/array/Language"(_id) ON DELETE CASCADE
#|     "types/array/Language/:list/countries[]_countries[]._id_fkey" FOREIGN KEY ("countries[]._id") REFERENCES "types/array/Country"(_id) ON DELETE CASCADE

# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Language?format(ascii)"
#| HTTP/1.1 200 OK
#|
#| http: error: ChunkedEncodingError: ("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)", InvalidChunkLength(got length b'', 0 bytes read))
# TODO: https://github.com/atviriduomenys/spinta/issues/360


http POST "$SERVER/$DATASET/Language" $AUTH name=Lithuanian
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "6a2c388f-3aea-42d4-8dcc-7534d6367e81",
#|     "_revision": "02be4400-f991-494d-bdc1-3824ae9edbfc",
#|     "_type": "types/array/Language",
#|     "countries[]": null,
#|     "name": "Lithuanian"
#| }


http POST "$SERVER/$DATASET/Country" $AUTH <<'EOF'
{
    "name": "Lithuania",
    "languages": [
        {"_id": "6a2c388f-3aea-42d4-8dcc-7534d6367e81"}
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
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 94, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 201, in create_http_response
#|     return await commands.push(
#|   File "spinta/commands/write.py", line 100, in push
#|     status_code, response = await simple_response(
#|   File "spinta/commands/write.py", line 1122, in simple_response
#|     results = await alist(aslice(results, 2))
#|   File "spinta/utils/aiotools.py", line 51, in alist
#|     return [x async for x in it]
#|   File "spinta/utils/aiotools.py", line 51, in <listcomp>
#|     return [x async for x in it]
#|   File "spinta/utils/aiotools.py", line 62, in aslice
#|     async for x in it:
#|   File "spinta/accesslog/__init__.py", line 174, in log_async_response
#|     async for row in rows:
#|   File "spinta/commands/write.py", line 185, in push_stream
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/changes.py", line 25, in create_changelog_entry
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/write.py", line 23, in insert
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 845, in prepare_data_for_write
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 646, in prepare_patch
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 616, in validate_data
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 519, in read_existing_data
#|     async for data in dstream:
#|   File "spinta/commands/write.py", line 507, in prepare_data
#|     data.given = commands.load(context, data.model, data.payload)
#|   File "spinta/types/model.py", line 336, in load
#|     exceptions.FieldNotInResource(model, property=prop)
#| spinta.exceptions.MultipleErrors: Multiple errors:
#|  - Unknown property 'languages'.
#|      Context:
#|        component: spinta.components.Model
#|        manifest: default
#|        schema: 8
#|        dataset: types/array
#|        model: types/array/Country
#|        entity: 
#|        property: languages


http GET "$SERVER/$DATASET/Language"
#| HTTP/1.1 404 Not Found
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "PropertyNotFound",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.types.datatype.BackRef",
#|                 "dataset": "types/array",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/array/Language",
#|                 "property": "countries[]",
#|                 "schema": "4",
#|                 "type": "backref"
#|             },
#|             "message": "Property 'countries[]' not found.",
#|             "template": "Property {property!r} not found.",
#|             "type": "type"
#|         }
#|     ]
#| }
# TODO: https://github.com/atviriduomenys/spinta/issues/96
# FIXME: Do not include `backref` properties in the result, unless explicityly
#        selected.


tail -80 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 94, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 153, in create_http_response
#|     return await commands.getall(
#|   File "spinta/commands/read.py", line 121, in getall
#|     return render(context, request, model, params, rows, action=action)
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|   File "spinta/formats/json/commands.py", line 25, in render
#|     return _render(
#|   File "spinta/formats/json/commands.py", line 52, in _render
#|     aiter(peek_and_stream(fmt(data))),
#|   File "spinta/utils/response.py", line 223, in peek_and_stream
#|     peek = list(itertools.islice(stream, 2))
#|   File "spinta/formats/json/components.py", line 16, in __call__
#|     for i, row in enumerate(data):
#|   File "spinta/accesslog/__init__.py", line 162, in log_response
#|     for row in rows:
#|   File "spinta/commands/read.py", line 106, in <genexpr>
#|     rows = (
#|   File "spinta/backends/postgresql/commands/read.py", line 51, in getall
#|     expr = env.resolve(query)
#|   File "spinta/core/ufuncs.py", line 242, in resolve
#|     return ufunc(self, expr)
#|   File "spinta/backends/postgresql/commands/query.py", line 243, in select
#|     env.call('select', Star())
#|   File "spinta/backends/postgresql/commands/query.py", line 255, in select
#|     env.select[prop.place] = env.call('select', prop.dtype)
#|   File "spinta/backends/postgresql/commands/query.py", line 284, in select
#|     column = env.backend.get_column(table, dtype.prop, select=True)
#|   File "spinta/backends/postgresql/components.py", line 119, in get_column
#|     raise PropertyNotFound(prop.dtype)
#| spinta.exceptions.PropertyNotFound: Property 'countries[]' not found.
#|   Context:
#|     component: spinta.types.datatype.BackRef
#|     manifest: default
#|     schema: 4
#|     dataset: types/array
#|     model: types/array/Language
#|     entity: 
#|     property: countries[]
#|     attribute: 
#|     type: backref


http GET "$SERVER/$DATASET/Country"
