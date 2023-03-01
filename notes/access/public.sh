# 2023-02-17 10:59

git log -1 --oneline
#| bb34a00 (HEAD -> access-public, origin/master, origin/HEAD, origin/208-add-possibility-to-reference-external-model, master) Merge pull request #342 from at>

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=access/public
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | access
$DATASET                    |         | 
  |   |   | Person          |         | 
  |   |   |   | name        | string  | public
  |   |   |   | age         | integer | open
EOF

poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Person"'
#| name      | text                        |           |          | 
#| age       | integer                     |           |          | 

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Person" $AUTH \
    name="John" \
    age:=42

http GET "$SERVER/$DATASET/Person?format(ascii)" $AUTH
#| Table: access/public/Person
#| ------------------------------------  ----  ---
#| _id                                   name  age
#| 051603af-30a9-4c88-8bc2-f0817dc2c65d  John  42
#| ------------------------------------  ----  ---

http GET "$SERVER/$DATASET/Person?format(ascii)"
#| Table: access/public/Person
#| ------------------------------------  ---
#| _id                                   age
#| 051603af-30a9-4c88-8bc2-f0817dc2c65d  42
#| ------------------------------------  ---

http GET "$SERVER/$DATASET/Person?select(name,age)&format(ascii)"
#| HTTP/1.1 200 OK
#| 
#| http: error:
#| ChunkedEncodingError:
#| Connection broken:
#| InvalidChunkLength(got length b'', 0 bytes read)

tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "starlette/middleware/exceptions.py", line 68, in __call__
#|     await self.app(scope, receive, sender)
#|   File "starlette/routing.py", line 706, in __call__
#|     await route.handle(scope, receive, send)
#|   File "starlette/routing.py", line 276, in handle
#|     await self.app(scope, receive, send)
#|   File "starlette/routing.py", line 69, in app
#|     await response(scope, receive, send)
#|   File "starlette/responses.py", line 266, in __call__
#|     async with anyio.create_task_group() as task_group:
#|   File "anyio/_backends/_asyncio.py", line 662, in __aexit__
#|     raise exceptions[0]
#|   File "starlette/responses.py", line 269, in wrap
#|     await func()
#|   File "starlette/responses.py", line 258, in stream_response
#|     async for chunk in self.body_iterator:
#|   File "spinta/utils/response.py", line 233, in aiter
#|     for data in stream:
#|   File "spinta/formats/ascii/components.py", line 38, in __call__
#|     peek = next(data, None)
#|   File "spinta/accesslog/__init__.py", line 162, in log_response
#|     for row in rows:
#|   File "spinta/commands/read.py", line 106, in <genexpr>
#|     rows = (
#|   File "spinta/backends/postgresql/commands/read.py", line 51, in getall
#|     expr = env.resolve(query)
#|   File "spinta/core/ufuncs.py", line 242, in resolve
#|     return ufunc(self, expr)
#|   File "multipledispatch/dispatcher.py", line 278, in __call__
#|     return func(*args, **kwargs)
#|   File "spinta/backends/postgresql/commands/query.py", line 239, in select
#|     selected = env.call('select', arg)
#|   File "spinta/core/ufuncs.py", line 216, in call
#|     return ufunc(self, *args, **kwargs)
#|   File "multipledispatch/dispatcher.py", line 278, in __call__
#|     return func(*args, **kwargs)
#|   File "spinta/backends/postgresql/commands/query.py", line 262, in select
#|     prop = _get_property_for_select(env, arg.name)
#|   File "spinta/backends/postgresql/commands/query.py", line 271, in _get_property_for_select
#|     raise FieldNotInResource(env.model, property=name)
#| spinta.exceptions.FieldNotInResource: Unknown property 'name'.
#|   Context:
#|     component: spinta.components.Model
#|     manifest: default
#|     schema: 4
#|     dataset: access/public
#|     model: access/public/Person
#|     entity: 
#|     property: name
#| 
#| 
#| The above exception was the direct cause of the following exception:
#| 
#| Traceback (most recent call last):
#|   File "uvicorn/protocols/http/h11_impl.py", line 404, in run_asgi
#|     result = await app(  # type: ignore[func-returns-value]
#|   File "uvicorn/middleware/proxy_headers.py", line 78, in __call__
#|     return await self.app(scope, receive, send)
#|   File "starlette/applications.py", line 124, in __call__
#|     await self.middleware_stack(scope, receive, send)
#|   File "starlette/middleware/errors.py", line 184, in __call__
#|     raise exc
#|   File "starlette/middleware/errors.py", line 162, in __call__
#|     await self.app(scope, receive, _send)
#|   File "spinta/middlewares.py", line 29, in __call__
#|     await self.app(scope, receive, send)
#|   File "starlette/middleware/exceptions.py", line 83, in __call__
#|     raise RuntimeError(msg) from exc
#| RuntimeError: Caught handled exception, but response already started.
