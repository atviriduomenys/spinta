# Configure psql
cat ~/.pgpass
cat >> ~/.pgpass <<'EOF'
localhost:54321:spinta:admin:admin123
localhost:54321:spinta_tests:admin:admin123
EOF


# Show tables
export PAGER="less -FSRX"
psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'


# Reset database
psql -h localhost -p 54321 -U admin spinta <<EOF
BEGIN TRANSACTION;
  DROP SCHEMA "public" CASCADE;
  CREATE SCHEMA "public";
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF

