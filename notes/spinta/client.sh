# Configure client
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
