from spinta import commands
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.utils import create_manifest_files
from spinta.testing.context import create_test_context
from spinta.components import Model, Context
from spinta.manifests.components import Manifest, get_manifest_object_names


def show(context: Context, c: Manifest):
    if isinstance(c, Manifest):
        res = {
            "type": c.type,
            "nodes": {},
        }
        for group in get_manifest_object_names():
            res["nodes"][group] = {
                name: show(context, node) for name, node in commands.get_nodes(context, c, group).items()
            }
            if not res["nodes"][group]:
                res["nodes"].pop(group)
        return res
    if isinstance(c, Model):
        return {
            "backend": c.backend.name,
        }


def test_manifest_loading(postgresql, rc, cli: SpintaCliRunner, tmp_path, request):
    rc = rc.fork(
        {
            "manifest": "default",
            "manifests": {
                "default": {
                    "type": "backend",
                    "sync": "yaml",
                },
                "yaml": {
                    "type": "yaml",
                    "path": str(tmp_path),
                },
            },
            "backends": ["default"],
        }
    )

    create_manifest_files(
        tmp_path,
        {
            "country.yml": {
                "type": "model",
                "name": "Country",
                "properties": {
                    "name": {"type": "string"},
                },
            },
        },
    )
    context = create_test_context(rc)

    config = context.get("config")
    commands.load(context, config)

    store = context.get("store")
    commands.load(context, store)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.prepare(context, store.manifest)

    request.addfinalizer(context.wipe_all)

    assert show(context, store.manifest) == {
        "type": "backend",
        "nodes": {
            "ns": {
                "": None,
                "_schema": None,
            },
            "model": {
                "_ns": {
                    "backend": "default",
                },
                "_schema": {
                    "backend": "default",
                },
                "_schema/Version": {
                    "backend": "default",
                },
                "_txn": {
                    "backend": "default",
                },
                "Country": {
                    "backend": "default",
                },
            },
        },
    }
