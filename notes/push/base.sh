# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/base
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type    | ref  | source                       | access
$DATASET                   |         |      |                              |
  | db                     | sql     |      | sqlite:///$BASEDIR/db.sqlite |
  |   |   |   |            |         |      |                              |     
  |   |   | Location       |         | id   | locations                    |
  |   |   |   | id         | integer |      | id                           | open
  |   |   |   | name       | string  |      | name                         | open
  |   |   |   | population | integer |      | population                   | open
  |   |   |   |            |         |      |                              |     
  |   | Location           |         | name |                              |
  |   |   | City           |         | id   | cities                       |
  |   |   |   | id         | integer |      | id                           | open
  |   |   |   | name       |         |      | name                         | open
  |   |   |   | population | integer |      | population                   | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS locations (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    population   INTEGER
);
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    population   INTEGER
);

INSERT INTO locations VALUES (1, 'Vilnius', 42);
INSERT INTO cities VALUES (10, 'Vilnius', 24);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM locations;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http DELETE "$SERVER/$DATASET/:wipe" $AUTH
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NotImplementedError",
#|             "message": "Could not find signature for authorize: <Context, Action, Base>"
#|         }
#|     ]
#| }
tail -50 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 170, in create_http_response
#|     return await commands.wipe(
#|            ^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/write.py", line 1334, in wipe
#|     commands.authorize(context, Action.WIPE, model)
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for authorize: <Context, Action, Base>
# FIXME: For some reason, base model is passed to authorize command. Why? To my
#        understanding, only models should go through authorize? This needs to
#        be investigated, there might lurk an error.
test -e $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| {
#|     '_id': 'd35edc8f-59d2-40e2-af6a-add3d23dc86e',
#|     '_op': 'insert',
#|     '_type': 'push/base/Location',
#|     'id': 1,
#|     'name': 'Vilnius',
#|     'population': 42,
#| }
#| Server response (status=500):
#|     {
#|         'errors': [
#|             {
#|                 'code': 'NotImplementedFeature',
#|                 'context': {
#|                     'attribute': 'name',
#|                     'component': 'spinta.components.Property',
#|                     'dataset': 'push/base',
#|                     'entity': 'cities',
#|                     'feature': 'Ability to indirectly modify base parameters',
#|                     'manifest': 'default',
#|                     'model': 'push/base/City',
#|                     'property': 'name',
#|                     'resource': 'db',
#|                     'resource.backend': 'push/base/db',
#|                     'schema': '11',
#|                 },
#|                 'message': 'Ability to indirectly modify base parameters is not implemented yet.',
#|                 'template': '{feature} is not implemented yet.',
#|                 'type': 'property',
#|             },
#|         ],
#|     }
#| Error while reading data.
#| Traceback (most recent call last):
#|   File "sqlalchemy/engine/base.py", line 1771, in _execute_context
#|     self.dialect.do_execute(
#|   File "sqlalchemy/engine/default.py", line 717, in do_execute
#|     cursor.execute(statement, parameters)
#| sqlite3.IntegrityError: UNIQUE constraint failed: push/base/City.id
#| 
#| The above exception was the direct cause of the following exception:
#| 
#| Traceback (most recent call last):
#|   File "spinta/cli/push.py", line 368, in _push_rows
#|     next(rows)
#|   File "spinta/cli/push.py", line 902, in _save_push_state
#|     conn.execute(
#|   File "sqlalchemy/engine/default.py", line 717, in do_execute
#|     cursor.execute(statement, parameters)
#| sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError)
#|     UNIQUE constraint failed: push/base/City.id
#| SQL:
#|     INSERT INTO "push/base/City"
#|         (id, checksum, revision, pushed, error, data)
#|     VALUES
#|         (?, ?, ?, ?, ?, ?)
#| parameters:
#|     (
#|         id:       'fb894fa0-2715-46a6-a998-1c3edb09bbe4',
#|         checksum: '0b20d169dead0daaa9f639a1c5d2c10cd4da59f2',
#|         revision: None,
#|         pushed:   '2023-09-14 14:27:07.289029',
#|         error:    1,
#|         data:     '{
#|                       "_type": "push/base/City",
#|                       "_id": "fb894fa0-2715-46a6-a998-1c3edb09bbe4",
#|                       "id": 10,
#|                       "name": "Vilnius",
#|                       "population": 24,
#|                       "_op": "insert"
#|                   }'
#|     )
tail -100 $BASEDIR/spinta.log
#| ERROR: Error: Ability to indirectly modify base parameters is not implemented yet.
#|   Context:
#|     component: spinta.components.Property
#|     manifest: default
#|     schema: 11
#|     dataset: push/base
#|     resource: db
#|     model: push/base/City
#|     entity: cities
#|     property: name
#|     attribute: name
#|     resource.backend: push/base/db
#|     feature: Ability to indirectly modify base parameters
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 95, in homepage
#|     return await create_http_response(context, params, request)
#|            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/utils/response.py", line 201, in create_http_response
#|     return await commands.push(
#|            ^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/write.py", line 96, in push
#|     status_code, response = await _batch_response(
#|                             ^^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/commands/write.py", line 1162, in _batch_response
#|     async for data in results:
#|   File "spinta/accesslog/__init__.py", line 174, in log_async_response
#|     async for row in rows:
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
#|   File "spinta/commands/write.py", line 510, in prepare_data
#|     commands.simple_data_check(context, data, data.model, data.model.backend)
#|   File "multipledispatch/dispatcher.py", line 278, in __call__
#|     return func(*args, **kwargs)
#|            ^^^^^^^^^^^^^^^^^^^^^
#|   File "spinta/backends/__init__.py", line 150, in simple_data_check
#|     simple_model_properties_check(context, data, model, backend)
#|   File "spinta/backends/__init__.py", line 177, in simple_model_properties_check
#|     simple_data_check(
#|   File "spinta/backends/__init__.py", line 290, in simple_data_check
#|     raise exceptions.NotImplementedFeature(prop, feature="Ability to indirectly modify base parameters")
#| spinta.exceptions.NotImplementedFeature: Ability to indirectly modify base parameters is not implemented yet.
#|   Context:
#|     component: spinta.components.Property
#|     manifest: default
#|     schema: 11
#|     dataset: push/base
#|     resource: db
#|     model: push/base/City
#|     entity: cities
#|     property: name
#|     attribute: name
#|     resource.backend: push/base/db
#|     feature: Ability to indirectly modify base parameters
#| 
#| INFO:     127.0.0.1:41448 - "POST / HTTP/1.1" 500 Internal Server Error
# FIXME: It looks, that there is a multiple errors here.
#        1. It looks, that push attempts to write to base (Location) model, by
#           sending `name` which is part of Location, and not part of City.
#           I think, that push should only send data, that directly belongs to
#           model and do not send properties, that comes from base.
#        2. push command couldn't correctly handle error received from remote
#           server and failed, when writing to push state table.
