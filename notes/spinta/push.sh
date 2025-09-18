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
  uapi:/:getone
  uapi:/:getall
  uapi:/:search
  uapi:/:changes
  uapi:/:create
  uapi:/:update
  uapi:/:patch
  uapi:/:delete
  uapi:/:wipe
EOF


