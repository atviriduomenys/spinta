# 2023-04-14 09:39

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=cli/inspect
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    continent_id INTEGER
);
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    country_id   INTEGER,
    FOREIGN KEY  (country_id) REFERENCES countries (id)
);
EOF
sqlite3 $BASEDIR/db.sqlite ".schema"

unset SPINTA_CONFIG
test -f $BASEDIR/config.yml && echo "Remove $BASEDIR/config.yml"

poetry run spinta config manifest
#| Origin  Name                       Value   
#| ------  -------------------------  --------
#| spinta  manifests.default.type     memory  
#| spinta  manifests.default.backend          
#| spinta  manifests.default.mode     internal
#| spinta  manifests.default.keymap           
#| spinta  manifest                   default 

poetry run spinta --tb=native inspect -r sql sqlite:///$BASEDIR/db.sqlite -o $BASEDIR/manifest.csv
#| Traceback (most recent call last):
#|   File "spinta/cli/inspect.py", line 65, in inspect
#|     store = load_manifest(context, ensure_config_dir=True)
#|   File "spinta/cli/helpers/store.py", line 99, in load_manifest
#|     store = load_store(
#|   File "spinta/cli/helpers/store.py", line 85, in load_store
#|     commands.load(context, store)
#|   File "spinta/types/store.py", line 50, in load
#|     manifest = create_manifest(context, store, manifest)
#|   File "spinta/manifests/helpers.py", line 68, in create_manifest
#|     Manifest_ = config.components['manifests'][mtype]
#| KeyError: 'memory'
