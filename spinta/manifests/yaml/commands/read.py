import jsonpatch

from spinta import spyna
from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.components import Context
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.yaml.helpers import list_yaml_files
from spinta.manifests.yaml.helpers import read_yaml_file


@commands.getall.register(Context, YamlManifest)
def getall(context: Context, manifest: YamlManifest, *, query: Expr = None):
    for file in list_yaml_files(manifest):
        versions = read_yaml_file(file)

        schema = next(versions, None)
        if schema:
            version = {
                '_type': '_schema/version',
                '_id': None,
                'created': None,
                'synced': None,
                'applied': None,
                'parents': None,
                'changes': None,
                'migrate': None,
                'type': schema['type'],
                'name': schema['name'],
                'sid': schema.get('id'),
                'schema': schema,
            }
            yield version

        schema = {}
        for version in versions:
            patch = version.get('changes', [])
            patch = jsonpatch.JsonPatch(patch)
            schema = patch.apply(schema)
            version = {
                '_type': '_schema/version',
                '_id': version['id'],
                'created': version['date'],
                'synced': None,
                'applied': None,
                'parents': version['parents'],
                'changes': version['changes'],
                'migrate': [
                    {
                        **action,
                        'upgrade': spyna.parse(action['upgrade']),
                        'downgrade': spyna.parse(action['downgrade']),
                    }
                    for action in version['migrate']
                ],
                'type': schema['type'],
                'name': schema['name'],
                'sid': schema['id'],
                'schema': schema,
            }
            yield version
