from spinta.formats.components import Format


class Html(Format):
    content_type = 'text/html'
    accept_types = {
        'text/html',
    }
    params = {}
    streamable = False
