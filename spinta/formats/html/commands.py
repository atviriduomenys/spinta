from typing import Any
from typing import Dict
from typing import Optional, Union

import pkg_resources as pres

from starlette.requests import Request
from starlette.templating import Jinja2Templates

from spinta.formats.html.components import Html
from spinta.formats.html.helpers import get_changes
from spinta.formats.html.helpers import get_data
from spinta.formats.html.helpers import get_ns_data
from spinta.formats.html.helpers import get_output_formats
from spinta.formats.html.helpers import get_row
from spinta.components import Context, Action, UrlParams
from spinta.formats.html.helpers import get_template_context
from spinta import commands
from spinta.components import Namespace, Model


def _render_check(request: Request, data: Dict[str, Any] = None):
    if data:
        if 'errors' in data:
            result = {
                'message': "Yra klaidų",
                'errors': [
                    err['message']
                    for err in data['errors']
                ],
            }
        else:
            result = {
                'message': "Klaidų nerasta.",
                'errors': None,
            }
    else:
        result = data

    templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
    return templates.TemplateResponse('form.html', {
        'request': request,
        'title': "Duomenų struktūros aprašo tikrinimas",
        'description': (
            "Ši priemonė leidžia patikrinti ar "
            "<a href=\"https://atviriduomenys.readthedocs.io/dsa/index.html\">"
            "duomenų struktūros apraše</a> nėra klaidų."
        ),
        'name': 'check',
        'fields': [
            {
                'label': "Duomenų struktūros aprašas",
                'help': "Pateikite duomenų struktūros aprašo failą.",
                'input': '<input type="file" name="manifest" accept=".csv">'
            },
        ],
        'submit': "Tikrinti",
        'result': result,
    })


@commands.render.register(Context, Request, Namespace, Html)
def render(
    context: Context,
    request: Request,
    ns: Namespace,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    if action == Action.CHECK:
        return _render_check(request, data)
    else:
        return _render_model(context, request, ns, action, params, data, headers)


@commands.render.register(Context, Request, Model, Html)
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    return _render_model(context, request, model, action, params, data, headers)


def _render_model(
    context: Context,
    request: Request,
    model: Union[Model, Namespace],
    action: Action,
    params: UrlParams,
    data,
    http_headers,
):
    header = []
    row = []

    if model.type == 'model:ns' or params.ns:
        data = get_ns_data(data)
        header = next(data)
        data = list(data)
    elif action == Action.CHANGES:
        data = get_changes(context, data, model, params)
        header = next(data)
        data = list(reversed(list(data)))
    elif action == Action.GETONE:
        row = list(get_row(context, data, model))
        data = []
    elif action in (Action.GETALL, Action.SEARCH):
        data = get_data(context, data, model, params, action)
        header = next(data)
        data = list(data)

    templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
    return templates.TemplateResponse(
        'data.html',
        {
            **get_template_context(context, model, params),
            'request': request,
            'header': header,
            'data': data,
            'row': row,
            'formats': get_output_formats(params),
            'limit_enforced': params.limit_enforced,
            'params': params,
        },
        headers=http_headers
    )
