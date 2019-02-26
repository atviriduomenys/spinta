import collections

import pkg_resources as pres
import uvicorn

from starlette.applications import Starlette
from starlette.templating import Jinja2Templates

from spinta.utils.tree import build_path_tree


templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))

app = Starlette()

store = None


@app.route('/{path:path}')
async def homepage(request):
    path = request.path_params['path'].strip('/')
    parts = path.split('/') if path else []

    datasets_by_object = collections.defaultdict(dict)
    for dataset in store.objects['default'].get('dataset', {}).values():
        for obj in dataset.objects.values():
            datasets_by_object[obj.name][dataset.name] = dataset

    loc = (
        [('root', '/')] +
        [(p, '/' + '/'.join(parts[i:])) for i, p in enumerate(parts[:-1], 1)] +
        [(p, None) for p in parts[-1:]]
    )

    # Resolve directory
    tree = build_path_tree(
        store.objects['default'].get('model', {}).keys(),
        datasets_by_object.keys(),
    )
    items = []
    if path in tree:
        items += [
            (item, ('/' if path else '') + path + '/' + item)
            for item in tree[path]
        ]

    # Resolve a dataset
    datasets = []
    if path in datasets_by_object:
        for name in sorted(datasets_by_object[path].keys()):
            datasets.append((
                name,
                ('/' if path else '') + path + '/' + name,
            ))

    return templates.TemplateResponse('base.html', {
        'request': request,
        'location': loc,
        'items': items,
        'datasets': datasets,
    })


def run(_store):
    global store
    store = _store
    uvicorn.run(app, host='0.0.0.0', port=8000)
