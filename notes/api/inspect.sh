unset SPINTA_CONFIG
unset SPINTA_CONFIG_PATH

INSTANCE=api/inspect
DATASET=$INSTANCE
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

cat > $BASEDIR/data.csv <<'EOF'
id,name
1,Vilnius
2,Kaunas
EOF

http -f POST "$SERVER/:inspect" resource.type=csv resource.file@$BASEDIR/data.csv
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NoBackendConfigured",
#|             "context": {
#|                 "component": "spinta.manifests.memory.components.MemoryManifest",
#|                 "manifest": "default",
#|                 "schema": "None"
#|             },
#|             "message": "Backend is not configured, can't proceed the request.",
#|             "template": "Backend is not configured, can't proceed the request.",
#|             "type": "memory"
#|         }
#|     ]
#| }
# FIXME: This should work.


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref     | access
$DATASET                    |         |         |
  |   |   | City            |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | name        | string  |         | open
EOF


http -f POST "$SERVER/:inspect" resource.type=tabular resource.file@$BASEDIR/manifest.txt
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NoBackendConfigured",
#|             "context": {
#|                 "component": "spinta.manifests.memory.components.MemoryManifest",
#|                 "manifest": "default",
#|                 "schema": "None"
#|             },
#|             "message": "Backend is not configured, can't proceed the request.",
#|             "template": "Backend is not configured, can't proceed the request.",
#|             "type": "memory"
#|         }
#|     ]
#| }
# FIXME: This should work.
