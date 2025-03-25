from typing import Any

import yaml

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.components import Context
from spinta.core.context import configure_context, create_context


def initialize_context_and_store(
    manifests: list[str], records: Any
) -> tuple[Context, Any]:
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

    return context, store


def getall_json_content_from_yaml(
    manifests: list[str], yaml_content: str, model_name: str
) -> dict:
    records = yaml.safe_load(yaml_content)
    context, store = initialize_context_and_store(manifests, records)
    commands.prepare(context, store.manifest)
    response_dict = {}
    model = commands.get_model(context, store.manifest, model_name)
    response_dict["_data"] = list(
        commands.getall(context, model, store.manifest.backend)
    )
    return response_dict


def getone_json_content_from_yaml(
    manifests: list[str], yaml_content: str, model_name: str, id_: str
) -> dict:
    records = yaml.safe_load(yaml_content)
    context, store = initialize_context_and_store(manifests, records)
    commands.prepare(context, store.manifest)
    model = commands.get_model(context, store.manifest, model_name)
    try:
        return commands.getone(context, model, store.manifest.backend, id_=id_)
    except KeyError:
        return {}
