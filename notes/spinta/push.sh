# Create temporary test directory
TEST_DIR=/tmp/spinta-push-test
mkdir -p $TEST_DIR/data


# Start PostgreSQL
docker compose up -d db


# Create source SQLite database with test data
python3 -c "
import sqlite3
conn = sqlite3.connect('$TEST_DIR/source.db')
conn.execute('CREATE TABLE IF NOT EXISTS country (id INTEGER PRIMARY KEY, code TEXT, name TEXT)')
conn.execute('DELETE FROM country')
conn.executemany('INSERT INTO country VALUES (?, ?, ?)', [
    (1, 'lt', 'Lithuania'),
    (2, 'lv', 'Latvia'),
    (3, 'pl', 'Poland'),
])
conn.commit()
conn.close()
print('Source database created with 3 rows')
"


# Create server manifest
cat > $TEST_DIR/manifest.csv <<'EOF'
id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
,datasets/test/push,,,,,,,,,,,,,
,,,,Country,,,code,,,,,,,
,,,,,code,string,,,,,open,,,
,,,,,name,string,,,,,open,,,
EOF


# Create client-side manifest
cat > $TEST_DIR/client-manifest.csv <<EOF
id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
,datasets/test/push,,,,,,,,,,,,,
,,data,,,,sql,,sqlite:///$TEST_DIR/source.db,,,,,,
,,,,Country,,,code,country,,,,,,
,,,,,code,string,,code,,,open,,,
,,,,,name,string,,name,,,open,,,
EOF


# Create server config
cat > $TEST_DIR/config.yml <<EOF
data_path: $TEST_DIR/data
default_auth_client: default
keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$TEST_DIR/data/keymap.db
backends:
  default:
    type: postgresql
    dsn: postgresql://admin:admin123@localhost:54321/spinta
manifest: default
manifests:
  default:
    type: csv
    path: $TEST_DIR/manifest.csv
    backend: default
    keymap: default
    mode: internal
accesslog:
  type: file
  file: stdout
EOF


# Generate auth keys and create a client
export SPINTA_CONFIG=$TEST_DIR/config.yml
poetry run spinta key generate
poetry run spinta upgrade clients
poetry run spinta client add \
  -n test \
  -s secret123 \
  --add-secret \
  --scope "uapi:/:set_meta_fields uapi:/:getone uapi:/:getall uapi:/:search uapi:/:create uapi:/:patch uapi:/:delete uapi:/:changes"


# Bootstrap and migrate the server database
poetry run spinta bootstrap
poetry run spinta migrate


# Start the server (separate terminal)
export SPINTA_CONFIG=/tmp/spinta-push-test/config.yml
poetry run uvicorn spinta.asgi:app --host 0.0.0.0 --port 8000 --log-level info


# Create client credentials file (back to client terminal)
cat > $TEST_DIR/credentials.cfg <<'EOF'
[localhost:8000]
client = test
secret = secret123
server = http://localhost:8000
scopes =
  uapi:/:set_meta_fields
  uapi:/:getone
  uapi:/:getall
  uapi:/:search
  uapi:/:create
  uapi:/:patch
  uapi:/:delete
  uapi:/:changes
EOF


# Push data (client -> server)
poetry run spinta push $TEST_DIR/client-manifest.csv \
  -o http://localhost:8000 \
  --credentials $TEST_DIR/credentials.cfg \
  --mode external \
  -a test


# Verify data was pushed
curl -s http://localhost:8000/datasets/test/push/Country | python3 -m json.tool

# Expected output:
#{
#    "_data": [
#        {
#            "_type": "datasets/test/push/Country",
#            "_id": "9ab13619-626a-40a4-b677-e1766d7d346c",
#            "_revision": "4cfcab3a-a186-4b4a-a1e7-573a32d26467",
#            "code": "lt",
#            "name": "Lithuania"
#        },
#        {
#            "_type": "datasets/test/push/Country",
#            "_id": "7e3a5f07-24f1-47cb-b7d3-1621062e04b7",
#            "_revision": "ee1cb745-5184-4b03-98a7-183e02afa43c",
#            "code": "lv",
#            "name": "Latvia"
#        },
#        {
#            "_type": "datasets/test/push/Country",
#            "_id": "e1544373-fc3d-4dd0-8e20-bca2b90d1825",
#            "_revision": "87c5ce29-5f55-4853-af96-e79d037ae45a",
#            "code": "pl",
#            "name": "Poland"
#        }
#    ],
#    "_page": {
#        "next": "WyJwbCIsICJlMTU0NDM3My1mYzNkLTRkZDAtOGUyMC1iY2EyYjkwZDE4MjUiXQ=="
#    }
#}

