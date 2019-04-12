from spinta.commands import load, prepare, check
from spinta.components import Context, Manifest, Node


class Project(Node):
    metadata = {
        'name': 'project',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'version': {'type': 'integer', 'required': True},
            'date': {'type': 'date', 'required': True},
            'objects': {'type': 'object', 'default': {}},
            'impact': {'type': 'array', 'default': []},
            'url': {'type': 'url'},
            'source_code': {'type': 'url'},
            'owner': {'type': 'string'},
            'parent': {'type': 'manifest'},
        },
    }


class Impact:
    metadata = {
        'name': 'project.impact',
        'properties': {
            'year': {'type': 'integer', 'required': True},
            'users': {'type': 'integer'},
            'revenue': {'type': 'number'},
            'employees': {'type': 'integer'},
            'parent': {'type': 'project'},
        },
    }


class Model(Node):
    metadata = {
        'name': 'project.model',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
            'parent': {'type': 'project'},
        },
    }

    property_type = 'project.property'


class Property(Node):
    metadata = {
        'name': 'project.property',
        'properties': {
            'enum': {'type': 'array'},
            'parent': {'type': 'project.model'},
        },
    }


@load.register()
def load(context: Context, project: Project, data: dict, manifest: Manifest):
    for name, obj in data.get('objects', {}).items():
        project.objects[name] = load(context, Model(), manifest, {
            'name': name,
            **(obj or {}),
        })

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
