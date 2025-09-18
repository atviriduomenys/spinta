# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=api/auth/clients
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type   | access
$DATASET                 |        | 
  |   |   | City         |        | 
  |   |   |   | name     | string | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server

# notes/spinta/client.sh    Configure client
SERVER=:8000
CLIENT=test
SECRET=secret
SCOPES=(
    spinta_set_meta_fields
    uapi:/:getone
    uapi:/:getall
    uapi:/:search
    uapi:/:changes
    uapi:/:create
    uapi:/:update
    uapi:/:patch
    uapi:/:delete
    uapi:/:wipe
    spinta_auth_clients
)
http \
    -a $CLIENT:$SECRET \
    -f $SERVER/auth/token \
    grant_type=client_credentials \
    scope="$SCOPES"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "error": "invalid_scope",
#|     "error_description": "The requested scope is invalid, unknown, or malformed."
#| }
tail -50 $BASEDIR/spinta.log
#| ERROR: Authorization server error: invalid_scope: 
#| Traceback (most recent call last):
#|   File "authlib/oauth2/rfc6749/authorization_server.py", line 185, in create_token_response
#|     grant.validate_token_request()
#|   File "authlib/oauth2/rfc6749/grants/client_credentials.py", line 72, in validate_token_request
#|     self.validate_requested_scope(client)
#|   File "authlib/oauth2/rfc6749/grants/base.py", line 92, in validate_requested_scope
#|     raise InvalidScopeError(state=self.request.state)
#| authlib.oauth2.rfc6749.errors.InvalidScopeError: invalid_scope: 
#| INFO: "POST /auth/token HTTP/1.1" 400 Bad Request

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
