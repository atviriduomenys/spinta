# Configure client
SERVER=:8000
CLIENT=test
SECRET=secret
SCOPES=(
    spinta_set_meta_fields
    spinta_getone
    spinta_getall
    spinta_search
    spinta_changes
    spinta_insert
    spinta_upsert
    spinta_update
    spinta_patch
    spinta_delete
    spinta_wipe
)

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

http GET    "$SERVER/:ns/:all?format(ascii,width(100))" $AUTH
