# 2023-02-15 14:38

git log -1 --oneline
#| 3da4c2c (HEAD -> 308-add-export-to-rdf-support, origin/308-add-export-to-rdf-support) 308 fix _stream

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=formats/rdf
# notes/spinta/server.sh    Configure server

http \
    -d https://github.com/atviriduomenys/manifest/raw/master/datasets/gov/ivpk/adk.csv \
    -o $BASEDIR/manifest.csv

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

DATASET=datasets/gov/ivpk/adp/catalog

http POST "$SERVER/$DATASET/Organization" $AUTH \
    _id="5191b15c-be68-41b5-83e7-b4b9cc15d56b" \
    title="Information Society Development Committee"

http POST "$SERVER/$DATASET/Catalog" $AUTH \
    _id="9d570f18-3201-4284-9ba7-9f59ba088ce6" \
    title="Open Data Portal"

http POST "$SERVER/$DATASET/Licence" $AUTH \
    _id="6af18deb-c203-4030-94a9-ee6e61dcf818" \
    identifier="CC-BY-4.0" \
    title="Creative Commons Attribution 4.0" \
    url="https://creativecommons.org/licenses/by/4.0/deed.lt"

http POST "$SERVER/$DATASET/Category" $AUTH \
    _id="0b77aa8d-eb75-4a80-9424-c8cc54f484fa" \
    title="Data resources" \
    description="Datasets aboult data."

http POST "$SERVER/$DATASET/Dataset" $AUTH <<'EOF'
{
    "_id": "55f32212-38c1-434b-99e1-5dbe0d4a43c4",
    "title": "Open data portal",
    "description": "A portal where metadata of open data are published.",
    "catalog": {"_id": "9d570f18-3201-4284-9ba7-9f59ba088ce6"},
    "category": {"_id": "0b77aa8d-eb75-4a80-9424-c8cc54f484fa"},
    "licence": {"_id": "6af18deb-c203-4030-94a9-ee6e61dcf818"},
    "creator": {"_id": "5191b15c-be68-41b5-83e7-b4b9cc15d56b"},
    "publisher": {"_id": "5191b15c-be68-41b5-83e7-b4b9cc15d56b"}
}
EOF

echo $BASEDIR/data.rdf
#| var/instances/formats/rdf/data.rdf

http GET "$SERVER/$DATASET/Dataset/:format/rdf" -o $BASEDIR/data.rdf
