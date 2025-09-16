# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=cli/migrate
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

git clone https://github.com/atviriduomenys/manifest.git ../manifest
git -C ../manifest status
git -C ../manifest co master
git -C ../manifest pull
git -C ../manifest log --oneline -n 100 | grep Merge
git -C ../manifest co 20959140

export BASEDIR=$PWD/var/instances/$INSTANCE
export SPINTA_CONFIG=$BASEDIR/config.yml

poetry shell
cd ../manifest
cat get_data_gov_lt.in | xargs spinta copy -o $BASEDIR/manifest.csv
exit

poetry run spinta --tb=native check $BASEDIR/manifest.csv
#| Traceback (most recent call last):
#|   File "spinta/cli/config.py", line 35, in check
#|     prepare_manifest(context, ensure_config_dir=True)
#|   File "spinta/cli/helpers/store.py", line 138, in prepare_manifest
#|     commands.prepare(context, store.manifest)
#|   File "spinta/manifests/commands/init.py", line 14, in prepare
#|     commands.prepare(context, backend, manifest)
#|   File "spinta/backends/postgresql/commands/init.py", line 24, in prepare
#|     commands.prepare(context, backend, model)
#|   File "spinta/backends/postgresql/commands/init.py", line 53, in prepare
#|     main_table = sa.Table(
#|                  ^^^^^^^^^
#|   File "sqlalchemy/sql/schema.py", line 3302, in _set_parent
#|     ColumnCollectionMixin._set_parent(self, table)
#|   File "sqlalchemy/sql/schema.py", line 3259, in _set_parent
#|     for col in self._col_expressions(table):
#|                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#|   File "sqlalchemy/sql/schema.py", line 3253, in _col_expressions
#|     return [
#|            ^
#|   File "sqlalchemy/sql/schema.py", line 3254, in <listcomp>
#|     table.c[col] if isinstance(col, util.string_types) else col
#|     ~~~~~~~^^^^^
#|   File "sqlalchemy/sql/base.py", line 1192, in __getitem__
#|     return self._index[key]
#|            ~~~~~~~~~~~^^^^^
#| KeyError: 'aob_kodas.aob_kodas'
