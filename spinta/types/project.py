from spinta.commands import load, prepare, check
from spinta.components import Context, Node, MetaData
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta import exceptions


class Project(MetaData):
    schema = {
        'version': {'type': 'integer', 'required': True},
        'date': {'type': 'date', 'required': True},
        'objects': {'type': 'object', 'default': {}},
        'impact': {'type': 'array', 'default': []},
        'url': {'type': 'url'},
        'source_code': {'type': 'url'},
        'website': {'type': 'url'},
        'owner': {'type': 'string'},
        'dataset': {'type': 'string'},
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
        'dataset': {'type': 'string', 'inherit': True},
        'target': {'type': 'string'},
    }


class Property(Node):
    schema = {
        'enum': {'type': 'array'},
        'dataset': {'type': 'string', 'inherit': True},
        'target': {'type': 'string'},
    }
    # TODO: inherit type from model if not provided, type is needed for data
    #       serialization.


@load.register(Context, Project, dict, Manifest)
def load(
    context: Context,
    project: Project,
    data: dict,
    manifest: Manifest,
    *,
    source: Manifest = None,
):
    load_node(context, project, data)
    project.impact = [
        {
            'year': None,
            'users': 0,
            'revenue': 0,
            'employees': 0,
            **impact,
        } for i, impact in enumerate(data.get('impact', []))
    ]


@prepare.register(Context, Project)
def prepare(context: Context, project: Project):
    for model in project.objects.values():
        prepare(context, model)


@check.register(Context, Project)
def check(context: Context, project: Project):
    if project.owner and project.owner not in project.manifest.objects['owner']:
        raise exceptions.UnknownOwner(project)

    if project.dataset and project.dataset not in project.manifest.objects['dataset']:
        # TODO add  similar 'dataset' checks for model and property.
        raise exceptions.UnknownProjectDataset(project)
