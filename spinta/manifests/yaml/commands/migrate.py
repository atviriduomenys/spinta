from spinta import commands
from spinta.components import Context
from spinta.manifests.yaml.components import YamlManifest


@commands.migrate.register(Context, YamlManifest)
def migrate(context: Context, manifest: YamlManifest):
    raise Exception(
        "Can't run migrations on 'yaml' manifest, use `spinta bootstrap` "
        "command instead."
    )
