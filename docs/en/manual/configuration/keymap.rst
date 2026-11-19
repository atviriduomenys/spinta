.. default-role:: literal

.. _keymap-configuration:

Configuring keymaps
###################

Keymap is used to map external identifiers with internal identifiers. Storage
(Backend) can be configured. By default a SQLite database is used, but it can
be changed to other, faster and more robust storages. Here full list of
options:

- SQLite database with SQLAlchemy backend configuration:

  .. code-block:: yaml

      keymaps:
        default:
          type: sqlalchemy
          dsn: sqlite:////path/to/keymap.db

- Redis persistent storage with Redis, configured like:

  .. code-block:: yaml

      keymaps:
        default:
          type: redis
          dsn: redis://redis-address:6379/1

Redis (valkey redis fork) docker run configuration can be found under project docker-compose.yml (root directory).
**IMPORTANT! Redis must be enabled in persistent mode (the `--appendonly yes --appendfsync always` parameter in docker-compose).**
There are several persistent modes (see the Redis/Valkey documentation).
Recommended approach (`--appendonly yes --appendfsync always`) provides the most durability and the least performance compared to the others.

A keymap for each manifest can be selected with the `keymap` option of a
manifest, see :ref:`manifest-configuration`. If `keymap` is not set, the
`default` keymap is used.
