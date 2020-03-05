from collections import defaultdict
from typing import Iterable, List

import datetime
import uuid

from ruamel.yaml import YAML
from toposort import toposort_flatten

import jsonpatch

from spinta.components import Context
from spinta.exceptions import MultipleParentsError
from spinta import commands


def build_migration_graph(versions: Iterable[dict]) -> dict:
    # Builds migration graph, where keys are schema version id and
    # values are a set of ids of the schema version children.
    # Root node id is `None`.
    graph = defaultdict(set)
    for version in versions:
        id_ = version['version']['id']
        parents = version['version']['parents']
        if not parents:
            graph[None].add(id_)
        else:
            for parent in parents:
                graph[parent].add(id_)
    return graph


def find_schema_leaf_ids(schema_graph: dict) -> List[str]:
    # DFS through directed schema graph, finds all leaf node ids
    leaf_ids = []
    # root node key is `None`
    stack = [None]
    discovered = set()
    while stack:
        node = stack.pop()
        if node not in discovered:
            discovered.add(node)
            leafs = schema_graph.get(node, set())

            # if no leafs - it means we are at a leaf
            # though there's no need to add `None` to leaf_ids if there's only
            # one schema version
            if not leafs and node is not None:
                leaf_ids.append(node)
            else:
                stack.extend(list(leafs))
    return leaf_ids


def find_parents(versions: Iterable[dict]) -> List[str]:
    schema_graph = build_migration_graph(versions[1:])
    leaf_ids = find_schema_leaf_ids(schema_graph)
    return leaf_ids


def get_ref_model_names(props: dict) -> List[str]:
    ref_model_names = []
    for prop in props.values():
        if prop.get('type') == 'ref':
            ref_model_names.append(prop['object'])
        elif prop.get('type') == 'array':
            array_items = get_ref_model_names({'_': prop['items']})
            ref_model_names.extend(array_items)
        elif prop.get('type') == 'object':
            ref_model_names.extend(get_ref_model_names(prop['properties']))
    return ref_model_names


def get_parents(versions: Iterable[dict], new: dict, context: Context) -> List[str]:
    # finds parents for the new schema version
    # parents are leaf nodes from the resource migration graph as well as
    # latest resource versions of referenced recourses (foreign keys)

    parents = find_parents(versions)
    # XXX: currently do not deal with migration merge situations
    if len(parents) > 1:
        raise MultipleParentsError()

    # find if migration has ref properties (foreign keys)
    properties = new.get('properties', {})
    ref_model_names = get_ref_model_names(properties)

    for ref_model_name in ref_model_names:
        # get latest version of foreign key resource and append it to parents
        store = context.get('store')
        manifest = store.manifests['default']
        ref_model = manifest.objects['model'][ref_model_name]
        ref_model_versions = list(YAML().load_all(ref_model.path.read_text()))
        ref_model_version = ref_model_versions[0]['version']['id']
        parents.append(ref_model_version)

    return parents


def get_schema_from_changes(versions: Iterable[dict]) -> dict:
    new = {}
    old = {}
    for i, version in enumerate(versions):
        if i == 0:
            new = version
            continue
        patch = version.get('changes')
        if patch:
            patch = jsonpatch.JsonPatch(patch)
            old = patch.apply(old)
    return old, new


def get_schema_changes(old: dict, new: dict) -> List[dict]:
    return [
        change
        for change in jsonpatch.make_patch(old, new)
        if not change['path'].startswith('/version/')
    ]


def get_new_schema_version(
    old: dict,
    changes: List[dict],
    migrate: dict,
    parents: List[str],
) -> dict:
    return {
        'version': {
            'id': str(uuid.uuid4()),
            'date': datetime.datetime.now(datetime.timezone.utc).astimezone(),
            'parents': parents,
        },
        'changes': changes,
        'migrate': {
            'schema': migrate,
        },
    }


def build_schema_relation_graph(model_yaml_data: Iterable[dict]) -> dict:
    # Builds model graph, where keys are model names
    # and values are referenced model names
    graph = {}
    for model_name, yaml_data in model_yaml_data.items():
        props = yaml_data[0]['properties'].values()
        graph[model_name] = set()
        for prop in props:
            if prop.get('type') == 'ref':
                graph[model_name].add(prop.get('object'))
    return graph


def toposort_models(model_yaml_data: Iterable[dict]) -> dict:
    model_graph = build_schema_relation_graph(model_yaml_data)
    return toposort_flatten(model_graph, sort=True)


def freeze(context):
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 80
    yaml.explicit_start = False

    store = context.get('store')
    all_manifests = [store.internal, *store.manifests.values()]

    model_yaml_data = {}
    model_name_to_instance = {}
    # load all model yamls into memory as cache to avoid multiple file reads
    for manifest in all_manifests:
        models = manifest.objects['model'].values()
        for model in models:
            versions = list(yaml.load_all(model.path.read_text()))
            model_yaml_data[model.name] = versions
            # in model properties, ref type has a name of reference model later
            # we'll need to map back the name of the model to the model instance
            model_name_to_instance[model.name] = model

    sorted_model_names = toposort_models(model_yaml_data)

    for model_name in sorted_model_names:
        versions = model_yaml_data[model_name]
        versions = list(versions)
        model = model_name_to_instance[model_name]
        version = commands.new_schema_version(
            context, model.backend, model, versions=versions,
        )
        if version:
            vnum = version['version']['id']
            print(f"Updating to version {vnum}: {model.path}")
            versions[0]['version'] = version['version']
            versions.append(version)
            with model.path.open('w') as f:
                yaml.dump_all(versions, f)
