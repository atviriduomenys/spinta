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

ls $BASEDIR
tree ~/.config/spinta/clients

export SPINTA_CONFIG_PATH=$BASEDIR/config
mkdir -p $BASEDIR/config
poetry run spinta key generate
#| Private key saved to var/instances/access/public/config/keys/private.json.
#| Public key saved to var/instances/access/public/config/keys/public.json.

# Add default client
poetry run spinta client add -n default --add-secret --scope - <<EOF
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
EOF
#| var/instances/access/public/config/clients/id/25/f1/517c-6a4b-4b06-a085-dae6948dc36e.yml
tree $BASEDIR/config/clients
cat $BASEDIR/config/clients/id/25/f1/517c-6a4b-4b06-a085-dae6948dc36e.yml
#| client_id: 25f1517c-6a4b-4b06-a085-dae6948dc36e
#| client_name: default
#| client_secret: GgJyXChyCddxf_L3PJlt3gdeo8y0mHjp
#| client_secret_hash: pbkdf2$sha256$180930$4lNWdwRCFA02HWHh9v0x_g$cBmQ8gO6FwZzcC-qMA9mQo0oYi-NDliSHz-lQShEEwQ
#| scopes:
#|   - uapi:/:getone
#|   - uapi:/:getall
#|   - uapi:/:search
#|   - uapi:/:changes
cat $BASEDIR/config/clients/helpers/keymap.yml
#| default: 25f1517c-6a4b-4b06-a085-dae6948dc36e
mv $BASEDIR/config/clients/id/25/f1/517c-6a4b-4b06-a085-dae6948dc36e.yml $BASEDIR/config/clients/default.yml
tree $BASEDIR/config/clients
cat $BASEDIR/config/clients/default.yml

# Add client
poetry run spinta client add -n test -s secret --add-secret --scope - <<EOF
uapi:/:set_meta_fields
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
uapi:/:create
uapi:/:update
uapi:/:patch
uapi:/:delete
uapi:/:wipe
uapi:/:auth_clients
EOF
#| var/instances/access/public/config/clients/id/0b/f0/3b54-eba0-4045-aade-d798d3015aba.yml
tree $BASEDIR/config/clients
cat $BASEDIR/config/clients/helpers/keymap.yml
#| default: 25f1517c-6a4b-4b06-a085-dae6948dc36e
#| test: 0bf03b54-eba0-4045-aade-d798d3015aba
cat $BASEDIR/config/clients/id/0b/f0/3b54-eba0-4045-aade-d798d3015aba.yml
mv $BASEDIR/config/clients/id/0b/f0/3b54-eba0-4045-aade-d798d3015aba.yml $BASEDIR/config/clients/test.yml
tree $BASEDIR/config/clients
rm -r $BASEDIR/config/clients/id $BASEDIR/config/clients/helpers

tree $BASEDIR/config/clients
#| var/instances/access/public/config/clients
#| ├── default.yml
#| └── test.yml

cat $BASEDIR/config/clients/default.yml
cat > $BASEDIR/config/clients/default.yml <<'EOF'
client_id: default
client_secret: secret
client_secret_hash: pbkdf2$sha256$346842$yLpG_ganZxGDuwzIsED4_Q$PBAqfikg6rvXzg2_s74zIPlGGilA5MZpyCyTjlEuzfI
scopes:
  - uapi:/:getone
  - uapi:/:getall
  - uapi:/:search
  - uapi:/:changes
EOF

cat $BASEDIR/config/clients/test.yml
cat > $BASEDIR/config/clients/test.yml <<'EOF'
client_id: test
client_secret: secret
client_secret_hash: pbkdf2$sha256$346842$yLpG_ganZxGDuwzIsED4_Q$PBAqfikg6rvXzg2_s74zIPlGGilA5MZpyCyTjlEuzfI
scopes:
  - uapi:/:set_meta_fields
  - uapi:/:getone
  - uapi:/:getall
  - uapi:/:search
  - uapi:/:changes
  - uapi:/:create
  - uapi:/:update
  - uapi:/:patch
  - uapi:/:delete
  - uapi:/:wipe
  - uapi:/:auth_clients
EOF

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server

SERVER=:8000
CLIENT=test
SECRET=secret
SCOPES=(
    uapi:/:set_meta_fields
    uapi:/:getone
    uapi:/:getall
    uapi:/:search
    uapi:/:changes
    uapi:/:create
    uapi:/:update
    uapi:/:patch
    uapi:/:delete
    uapi:/:wipe
    uapi:/:auth_clients
)
http -a $CLIENT:$SECRET -f $SERVER/auth/token grant_type=client_credentials scope="$SCOPES"
TOKEN=$(
    http \
        -a $CLIENT:$SECRET \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="$SCOPES" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH

tree $BASEDIR/config/clients
#| var/instances/access/public/config/clients
#| ├── default.yml
#| ├── helpers
#| │   └── keymap.yml
#| ├── id
#| │   ├── 7e
#| │   │   └── 1c
#| │   │       └── 0625-fd42-4215-bd86-f0ddff04fda1.yml
#| │   └── 80
#| │       └── b7
#| │           └── 3a02-33e5-46a6-af73-abd5ce4f283d.yml
#| └── test.yml
cat $BASEDIR/config/clients/test.yml
#| client_id: test
#| client_secret: secret
#| client_secret_hash: pbkdf2$sha256$346842$yLpG_ganZxGDuwzIsED4_Q$PBAqfikg6rvXzg2_s74zIPlGGilA5MZpyCyTjlEuzfI
#| scopes:
#|   - uapi:/:set_meta_fields
#|   - uapi:/:getone
#|   - uapi:/:getall
#|   - uapi:/:search
#|   - uapi:/:changes
#|   - uapi:/:create
#|   - uapi:/:update
#|   - uapi:/:patch
#|   - uapi:/:delete
#|   - uapi:/:wipe
#|   - uapi:/:auth_clients
cat $BASEDIR/config/clients/helpers/keymap.yml
# test: 80b73a02-33e5-46a6-af73-abd5ce4f283d
cat $BASEDIR/config/clients/id/80/b7/3a02-33e5-46a6-af73-abd5ce4f283d.yml
#| client_id: 80b73a02-33e5-46a6-af73-abd5ce4f283d
#| client_name: test
#| client_secret_hash: pbkdf2$sha256$346842$yLpG_ganZxGDuwzIsED4_Q$PBAqfikg6rvXzg2_s74zIPlGGilA5MZpyCyTjlEuzfI
#| scopes:
#|   - uapi:/:set_meta_fields
#|   - uapi:/:getone
#|   - uapi:/:getall
#|   - uapi:/:search
#|   - uapi:/:changes
#|   - uapi:/:create
#|   - uapi:/:update
#|   - uapi:/:patch
#|   - uapi:/:delete
#|   - uapi:/:wipe
#|   - uapi:/:auth_clients

http -a test:$SECRET -f $SERVER/auth/token grant_type=client_credentials scope="$SCOPES"
http -a 80b73a02-33e5-46a6-af73-abd5ce4f283d:$SECRET -f $SERVER/auth/token grant_type=client_credentials scope="$SCOPES"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "error": "invalid_client",
#|     "error_description": "Invalid client name"
#| }

http GET "$SERVER/auth/clients"
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientPermission",
#|             "context": {
#|                 "scope": "auth_clients"
#|             },
#|             "message": "You need to have 'auth_clients' in order to access this API endpoint.",
#|             "template": "You need to have {scope!r} in order to access this API endpoint.",
#|             "type": "system"
#|         }
#|     ]
#| }

http GET "$SERVER/auth/clients" $AUTH
#| [
#|     {
#|         "client_id": "7e1c0625-fd42-4215-bd86-f0ddff04fda1",
#|         "client_name": "default"
#|     },
#|     {
#|         "client_id": "80b73a02-33e5-46a6-af73-abd5ce4f283d",
#|         "client_name": "test"
#|     }
#| ]


http GET "$SERVER/auth/clients/80b73a02-33e5-46a6-af73-abd5ce4f283d"
#| HTTP/1.1 403 Forbidden

http GET "$SERVER/auth/clients/80b73a02-33e5-46a6-af73-abd5ce4f283d" $AUTH
#| {
#|     "client_id": "80b73a02-33e5-46a6-af73-abd5ce4f283d",
#|     "client_name": "test",
#|     "scopes": [
#|         "uapi:/:update",
#|         "uapi:/:changes",
#|         "uapi:/:wipe",
#|         "uapi:/:auth_clients",
#|         "uapi:/:getone",
#|         "uapi:/:set_meta_fields",
#|         "uapi:/:patch",
#|         "uapi:/:create",
#|         "uapi:/:search",
#|         "uapi:/:delete",
#|         "uapi:/:getall"
#|     ]
#| }


http POST "$SERVER/auth/clients" <<'EOF'
{
    "scopes": [
        "uapi:/:getall",
        "uapi:/:getone",
        "uapi:/:search"
    ]
}
EOF
#| HTTP/1.1 403 Forbidden

http POST "$SERVER/auth/clients" $AUTH <<'EOF'
{
    "secret": "verysecret",
    "scopes": [
        "uapi:/:getall",
        "uapi:/:getone",
        "uapi:/:search"
    ]
}
EOF
#| {
#|     "client_id": "ce39382b-edcb-4c3c-af12-70506e724086",
#|     "client_name": "ce39382b-edcb-4c3c-af12-70506e724086",
#|     "scopes": [
#|         "uapi:/:getall",
#|         "uapi:/:getone",
#|         "uapi:/:search"
#|     ]
#| }

http GET "$SERVER/auth/clients" $AUTH
#| [
#|     {
#|         "client_id": "ce39382b-edcb-4c3c-af12-70506e724086",
#|         "client_name": "ce39382b-edcb-4c3c-af12-70506e724086"
#|     },
#|     ...
#| ]

tree $BASEDIR/config/clients
#| var/instances/access/public/config/clients
#| └── id
#|     └── ce
#|         └── 39
#|             └── 382b-edcb-4c3c-af12-70506e724086.yml

cat $BASEDIR/config/clients/id/ce/39/382b-edcb-4c3c-af12-70506e724086.yml
#| client_id: ce39382b-edcb-4c3c-af12-70506e724086
#| client_name: ce39382b-edcb-4c3c-af12-70506e724086
#| client_secret_hash: pbkdf2$sha256$950424$FadCqxvtoEDNypZ1EWu5_A$NDx-WpH4g08qYq-wNgvVPFPPeOSUmbmeStjndA22sg4
#| scopes:
#|   - uapi:/:getall
#|   - uapi:/:getone
#|   - uapi:/:search

http -a ce39382b-edcb-4c3c-af12-70506e724086:verysecret -f $SERVER/auth/token grant_type=client_credentials scope="uapi:/:getall"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiJ9.eyJpc3MiOiJodHRwczovL2V4YW1wbGUuY29tLyIsInN1YiI6ImNlMzkzODJiL
#| WVkY2ItNGMzYy1hZjEyLTcwNTA2ZTcyNDA4NiIsImF1ZCI6ImNlMzkzODJiLWVkY2ItNGMzYy1hZjEyLTcwNTA2ZTcyNDA4NiIsImlhdCI6MTY5OTg2OTA5
#| OSwiZXhwIjoxNzAwNzMzMDk5LCJzY29wZSI6InNwaW50YV9nZXRhbGwifQ.PTtC7a4XNnkFOn9d7hkpQlRJqjfIrN7hI7aKfeq-5bD4TfVOnEGNzq3gAR8S
#| oV_RybpDBrDAnqta7-QFr474NcBZoffBJxw4Rgv360gKauubdz2avDFdK1ZAyzZqjaGy9OT8clLSJPtMzrjMA44VwZrDBqOTUpNhThwvwIxIKHvtBIojk81
#| HwY71ALtL0tc_0key_pGokBm_qhW2tDFTOr22Is5n0yG5s2cRHZRoFNz0NRR97olENKfvh3xTLkGGf-tNL6ad705WsMnJqzEL_vBjgFdpM1e_Nk8_dZgPxf
#| y2DN7zuIWGvvQqKF3uFKetcN2UHxhzsoIIq-pCX9XLjA",
#|     "expires_in": 864000,
#|     "scope": "uapi:/:getall",
#|     "token_type": "Bearer"
#| }


poetry run spinta client add -n new --scope uapi:/:getall
#| var/instances/access/public/config/clients/id/2e/a8/e9e8-92db-4820-a51a-bdbae49e31cb.yml
tree $BASEDIR/config/clients
#| var/instances/access/public/config/clients
#| ├── helpers
#| │   └── keymap.yml
#| └── id
#|     └── 2e
#|         └── a8
#|             └── e9e8-92db-4820-a51a-bdbae49e31cb.yml

http GET "$SERVER/auth/clients" $AUTH
#| [{"client_id": "2ea8e9e8-92db-4820-a51a-bdbae49e31cb", "client_name": "new"}}
http GET "$SERVER/auth/clients/2ea8e9e8-92db-4820-a51a-bdbae49e31cb" $AUTH
#| {
#|     "client_id": "2ea8e9e8-92db-4820-a51a-bdbae49e31cb",
#|     "client_name": "new",
#|     "scopes": [
#|         "uapi:/:getall"
#|     ]
#| }
http PATCH "$SERVER/auth/clients/2ea8e9e8-92db-4820-a51a-bdbae49e31cb" $AUTH <<EOF
{
    "scopes": [
        "uapi:/:getall",
        "uapi:/:search"
    ]
}
EOF
#| {
#|     "client_id": "2ea8e9e8-92db-4820-a51a-bdbae49e31cb",
#|     "client_name": "new",
#|     "scopes": [
#|         "uapi:/:getall",
#|         "uapi:/:search"
#|     ]
#| }
http GET "$SERVER/auth/clients/2ea8e9e8-92db-4820-a51a-bdbae49e31cb" $AUTH
#| {
#|     "client_id": "2ea8e9e8-92db-4820-a51a-bdbae49e31cb",
#|     "client_name": "new",
#|     "scopes": [
#|         "uapi:/:search",
#|         "uapi:/:getall"
#|     ]
#| }
http DELETE "$SERVER/auth/clients/2ea8e9e8-92db-4820-a51a-bdbae49e31cb" $AUTH
#| HTTP/1.1 204 No Content
http GET "$SERVER/auth/clients" $AUTH

http PATCH "$SERVER/auth/clients/ce39382b-edcb-4c3c-af12-70506e724086" $AUTH <<EOF
{
    "client_name": "old"
}
EOF
#| {
#|     "client_id": "ce39382b-edcb-4c3c-af12-70506e724086",
#|     "client_name": "old",
#|     "scopes": [
#|         "uapi:/:search",
#|         "uapi:/:getone",
#|         "uapi:/:getall"
#|     ]
#| }
http get "$SERVER/auth/clients/ce39382b-edcb-4c3c-af12-70506e724086" $AUTH
#| {
#|     "client_id": "ce39382b-edcb-4c3c-af12-70506e724086",
#|     "client_name": "old",
#|     "scopes": [
#|         "uapi:/:search",
#|         "uapi:/:getone",
#|         "uapi:/:getall"
#|     ]
#| }


http POST "$SERVER/$DATASET/Person" $AUTH \
    name="John" \
    age:=42

http GET "$SERVER/$DATASET/Person?format(ascii)"
#| Table: access/public/Person
#| ------------------------------------  ---
#| _id                                   age
#| 27489d9c-0f05-463a-bcdb-d4b54f1fca27  42
#| ------------------------------------  ---


TOKEN=$(
    http \
        -a old:verysecret \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="uapi:/:search uapi:/:getone uapi:/:getall" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH


http GET "$SERVER/$DATASET/Person?format(ascii)" $AUTH
#| Table: access/public/Person
#| ------------------------------------  ----  ---
#| _id                                   name  age
#| 27489d9c-0f05-463a-bcdb-d4b54f1fca27  John  42
#| ------------------------------------  ----  ---

http DELETE "$SERVER/$DATASET/Person/27489d9c-0f05-463a-bcdb-d4b54f1fca27" $AUTH
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientScopeError",
#|             "message": "insufficient_scope: Missing one of scopes: uapi:/access/:delete, uapi:/access/public/:delete, uapi:/access/public/person/:delete, uapi:/:delete"
#|         }
#|     ]
#| }

http GET "$SERVER/auth/clients" $AUTH
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientPermission",
#|             "context": {
#|                 "scope": "auth_clients"
#|             },
#|             "message": "You need to have 'auth_clients' in order to access this API endpoint.",
#|             "template": "You need to have {scope!r} in order to access this API endpoint.",
#|             "type": "system"
#|         }
#|     ]
#| }

http POST "$SERVER/auth/clients" $AUTH <<'EOF'
{
    "secret": "verysecret",
    "scopes": [
        "uapi:/:getall",
        "uapi:/:getone",
        "uapi:/:search"
    ]
}
EOF
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientPermission",
#|             "context": {
#|                 "scope": "auth_clients"
#|             },
#|             "message": "You need to have 'auth_clients' in order to access this API endpoint.",
#|             "template": "You need to have {scope!r} in order to access this API endpoint.",
#|             "type": "system"
#|         }
#|     ]
#| }



TOKEN=$(
    http \
        -a default:secret \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="uapi:/:getall" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH


http GET "$SERVER/$DATASET/Person?format(ascii)" $AUTH
#| Table: access/public/Person
#| ------------------------------------  ---
#| _id                                   age
#| 27489d9c-0f05-463a-bcdb-d4b54f1fca27  42
#| ------------------------------------  ---

TOKEN=$(
    http \
        -a $CLIENT:$SECRET \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="uapi:/:auth_clients" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH

http POST "$SERVER/auth/clients" $AUTH <<'EOF'
{
    "client_name": "example",
    "secret": "sample",
    "scopes": [
        "uapi:/datasets/example/:getall"
    ]
}
EOF

TOKEN=$(
    http \
        -a example:sample \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="uapi:/datasets/example/:getall" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH

http GET "$SERVER/$DATASET/Person?format(ascii)" $AUTH
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientScopeError",
#|             "message": "insufficient_scope: Missing one of scopes: uapi:/access/:getall, uapi:/access/public/:getall,
# uapi:/access/public/person/:getall, uapi:/:getall"
#|         }
#|     ]
#| }
# TODO: Should have same access as default client, should seen `age`, but not
#       `name`.
