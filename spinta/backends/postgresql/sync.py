import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.backends.postgresql import PostgreSQL


@commands.sync.register()
def sync(context: Context, backend: PostgreSQL):
    schema_table = sa.Table(
        '_schema', sa.MetaData(backend.engine),
        sa.Column('id', sa.Text, primary_key=True),  # migration version uuid
        sa.Column('created', sa.DateTime),
        sa.Column('updated', sa.DateTime),
        sa.Column('synced_time', sa.DateTime),
        sa.Column('applied', sa.DateTime),
        sa.Column('model', sa.Text),
        sa.Column('parents', sa.ARRAY(sa.Text)),
        sa.Column('changes', JSONB),
        sa.Column('actions', JSONB),
        sa.Column('schema', JSONB),
    )
    schema_table.create(checkfirst=True)

    conn = backend.engine.connect()

    # select all ids from migration table
    select_migration_ids = sa.sql.select([schema_table.c.id])
    result = conn.execute(select_migration_ids)
    ids = list(map(lambda x: x[0], result))

    yaml = YAML(typ="safe")
    dt_now = datetime.datetime.now(datetime.timezone.utc).astimezone()

    all_manifests = [store.internal, *store.manifests.values()]
    for manifest in all_manifests:
        for model in manifest.objects['model'].values():
            data = list(yaml.load_all(model.path))
            if len(data) == 1:
                log.warning(
                    f"Model '{model.name}' does not have migrations. "
                    f"Create migrations with `spinta freeze` first. "
                    f"YAML is not synced to DB."
                )
            version_schema = {}
            for migration in data[1:]:
                # make yaml migration data compatible with SQL
                migration = fix_data_for_json(migration)

                # build schema for current migration, so we will not need
                # to do this while querying _schema table
                changes = migration['changes']
                patch = jsonpatch.JsonPatch(changes)
                version_schema = patch.apply(version_schema)

                # if version is not in migrations table - save it
                version = migration['version']['id']
                if version not in ids:
                    # add version to migrations table
                    conn.execute(
                        schema_table.insert(),
                        id=version,
                        created=migration['version']['date'],
                        synced_time=dt_now.isoformat(),
                        model=model.name,
                        parents=migration['version'].get('parents', []),
                        changes=migration['changes'],
                        actions=migration['migrate']['schema'],
                        schema=version_schema,
                    )
                    log.info(
                        f"Synced migration '{version}' for model: '{model.name}"
                    )
