import json
import yaml

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.core.context import configure_context, create_context


def get_json_content_from_yaml(yaml_content: str, manifests: list[str]) -> str:
    records = yaml.safe_load(yaml_content)
    ctx = create_context()
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx, manifests)
    config = context.get("config")
    commands.load(context, config)
    commands.check(context, config)
    store = context.get("store")
    commands.load(context, store)
    commands.load(context, store.manifest)
    context.set("yaml_content", records)
    commands.prepare(context, store.manifest)
    response_dict = {}
    for model in commands.get_models(context, store.manifest).values():
        value = list(commands.getall(context, model, store.manifest.backend))
        if value:
            response_dict[value[0]["_type"]] = value
    return json.dumps(response_dict)
