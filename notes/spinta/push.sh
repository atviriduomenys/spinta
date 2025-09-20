# Configure client
cat ~/.config/spinta/credentials.cfg
cat >> ~/.config/spinta/credentials.cfg <<EOF

[test@localhost]
client = test
secret = secret
server = http://localhost:8000
scopes =
  spinta_set_meta_fields
  spinta_auth_clients
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
EOF


