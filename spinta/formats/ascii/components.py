import operator
import itertools

from spinta.components import Context, Action, UrlParams, Model
from spinta.manifests.components import Manifest
from spinta.formats.components import Format
from spinta.formats.ascii.helpers import draw
from spinta.utils.nestedstruct import flatten
from spinta.formats.helpers import get_model_tabular_header


class Ascii(Format):
    content_type = 'text/plain'
    accept_types = {
        'text/plain',
    }
    params = {
        'width': {'type': 'integer'},
        'colwidth': {'type': 'integer'},
    }

    def __call__(
        self,
        context: Context,
        model: Model,
        action: Action,
        params: UrlParams,
        data,
        width=None,
        colwidth=42,
    ):
        manifest: Manifest = context.get('store').manifest

        if action == Action.GETONE:
            data = [data]

        data = iter(data)
        peek = next(data, None)

        if peek is None:
            return

        data = itertools.chain([peek], data)

        if '_type' in peek:
            groups = itertools.groupby(data, operator.itemgetter('_type'))
        else:
            groups = [(None, data)]

        for name, group in groups:
            if name:
                yield f'\n\nTable: {name}\n'
                model = manifest.models[name]

            rows = flatten(group)
            cols = get_model_tabular_header(context, model, action, params)

            if colwidth:
                width = len(cols) * colwidth

            buffer = [cols]
            tnum = 1

            for row in rows:
                buffer.append([row.get(c) for c in cols])
                if len(buffer) > 100:
                    yield from draw(buffer, name, tnum, width)
                    buffer = [cols]
                    tnum += 1

            if buffer:
                yield from draw(buffer, name, tnum, width)
