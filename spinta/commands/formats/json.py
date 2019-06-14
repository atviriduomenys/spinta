import ujson as json

from spinta.commands.formats import Format
from spinta.components import Action


class Json(Format):
    content_type = 'application/json'
    accept_types = {
        'application/json',
    }
    params = {}
    wrap = 'data'

    def __call__(self, data, action: Action):
        wrap = action in (Action.GETALL, Action.SEARCH)
        if wrap:
            yield '{"' + self.wrap + '":['
            for i, item in enumerate(data):
                yield (',' if i > 0 else '') + json.dumps(item, ensure_ascii=False)
            yield ']}'
        else:
            yield json.dumps(data, ensure_ascii=False)
