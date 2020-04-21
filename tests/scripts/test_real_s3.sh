#!/bin/bash

# Client to authorize to the localhost:8000 `spinta` server and make
# requests to s3 backend
#
# client with id 'baa448a8-205c-4faa-a048-a10e4b32a136' should have these
# scopes added for this script to work:
#   - spinta_backends_postgres_s3_file_getall
#   - spinta_backends_postgres_s3_file_getone
#   - spinta_backends_postgres_s3_file_insert
#   - spinta_backends_postgres_s3_file_update
#   - spinta_backends_postgres_s3_file_file_update
#   - spinta_backends_postgres_s3_file_file_delete

if [ "$1" = "verbose" ]
then
    VERBOSITY=-vvv
else
    VERBOSITY=-s
fi

AUTH_RESP=$(curl $VERBOSITY 127.0.0.1:8000/auth/token \
    -u "baa448a8-205c-4faa-a048-a10e4b32a136:joWgziYLap3eKDL6Gk2SnkJoyz0F8ukB" \
    --data "grant_type=client_credentials" \
    --data "scope=spinta_backends_postgres_s3_file_getall spinta_backends_postgres_s3_file_getone spinta_backends_postgres_s3_file_insert spinta_backends_postgres_s3_file_update spinta_backends_postgres_s3_file_file_update spinta_backends_postgres_s3_file_file_delete")

TOKEN=`echo "$AUTH_RESP" | jq -r '.access_token'`

echo "*****************************"
echo "Creating s3_file"
CREATE_RESP=$(curl $VERBOSITY 127.0.0.1:8000/backends/postgres/s3_file \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    --data '{"title": "foobar"}')
echo $CREATE_RESP
echo "*****************************"

RESOURCE_ID=`echo "$CREATE_RESP" | jq -r ._id`
RESOURCE_REV=`echo "$CREATE_RESP" | jq -r ._revision`

echo "*****************************"
echo "Uploading file to s3_resource"
PUT_RESP=$(curl $VERBOSITY -X PUT 127.0.0.1:8000/backends/postgres/s3_file/${RESOURCE_ID}/file \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: text/plain" \
    -H "Revision: $RESOURCE_REV" \
    -H 'Content-Disposition: attachment; filename="foobar.txt"' \
    --data-binary @foobar.txt)
echo $PUT_RESP
echo "*****************************"

echo "*****************************"
echo "Getting file from s3_resource"
GET_RESP=$(curl $VERBOSITY 127.0.0.1:8000/backends/postgres/s3_file/${RESOURCE_ID}/file \
    -H "Authorization: Bearer $TOKEN" \
    -o get_foobar.txt)
echo $GET_RESP
echo "*****************************"

echo "*****************************"
ERROR=$(cmp foobar.txt get_foobar.txt 2>&1)
if [ $? -eq 0 ]
then
    echo "SUCCESS: foobar.txt matches get_foobar.txt"
else
    echo "FAIL: $ERROR"
fi
echo "*****************************"

echo "*****************************"
echo "Deleting file from s3_resource"
DELETE_RESP=$(curl $VERBOSITY -X DELETE 127.0.0.1:8000/backends/postgres/s3_file/${RESOURCE_ID}/file \
    -H "Authorization: Bearer $TOKEN")
echo $DELETE_RESP
echo "*****************************"
