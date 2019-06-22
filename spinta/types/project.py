from spinta.commands import load, prepare, check
from spinta.components import Context, Manifest, Node
from spinta.nodes import load_node


class Project(Node):
    schema = {
        'version': {'type': 'integer', 'required': True},
        'date': {'type': 'date', 'required': True},
        'objects': {'type': 'object', 'default': {}},
        'impact': {'type': 'array', 'default': []},
        'url': {'type': 'url'},
        'source_code': {'type': 'url'},
        'owner': {'type': 'string'},
    }


class Impact:
    schema = {
        'year': {'type': 'integer', 'required': True},
        'users': {'type': 'integer'},
        'revenue': {'type': 'number'},
        'employees': {'type': 'integer'},
        'parent': {'type': 'project'},
    }


class Model(Node):
    schema = {
        'properties': {'type': 'object', 'default': {}},
    }


class Property(Node):
    schema = {
        'enum': {'type': 'array'},
    }


@load.register()
def load(context: Context, project: Project, data: dict, manifest: Manifest):
    load_node(context, project, data, manifest)

    for name, obj in data.get('objects', {}).items():
        project.objects[name] = load_node(context, Model(), {
            'type': 'model',
            'name': name,
            'path': project.path,
            'parent': project,
            'backend': project.backend,
            **(obj or {}),
        }, manifest)

    project.impact = [
        {
            'year': None,
            'users': 0,
            'revenue': 0,
            'employees': 0,
            **impact,
        } for i, impact in enumerate(data.get('impact', []))
    ]


@prepare.register()
def prepare(context: Context, project: Project):
    for model in project.objects.values():
        prepare(context, model)


@check.register()
def check(context: Context, project: Project):
    if project.owner and project.owner not in project.manifest.objects['owner']:
        context.error(f"Unknown owner {project.owner}.")
