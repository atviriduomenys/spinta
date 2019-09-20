import itertools

from starlette.requests import Request

from spinta import commands
from spinta.components import Context, UrlParams, Namespace, Action, Model, Property
from spinta.renderer import render
from spinta.backends import Backend
from spinta.nodes import load_node, load_model_properties


@commands.check.register()
def check(context: Context, ns: Namespace):
    pass


@commands.getall.register()
async def getall(
    context: Context,
    request: Request,
    ns: Namespace,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
):
    data = [
        {
            'id': model.model_type(),
            'type': model.node_type(),
            'name': model.name,
            'specifier': model.model_specifier(),
            'title': model.title,
        }
        for model in itertools.chain(
            ns.names.values(),
            ns.models.values(),
        )
    ]
    data = sorted(data, key=lambda x: (x['type'] != 'ns', x['id']))

    schema = {
        'path': None,
        'parent': ns.manifest,
        'type': 'model:ns',
        'name': ns.model_type(),
        'properties': {
            'id': {'type': 'pk'},
            'type': {'type': 'string'},
            'name': {'type': 'string'},
            'specifier': {'type': 'string'},
            'title': {'type': 'string'},
        }
    }
    model = load_node(context, Model(), schema, ns.manifest)
    load_model_properties(context, model, Property, schema['properties'])

    return render(context, request, model, params, data, action=action)
