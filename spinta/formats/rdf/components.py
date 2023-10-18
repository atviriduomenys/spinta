from spinta.formats.components import Format


class Rdf(Format):
    content_type = 'application/rdf+xml'
    accept_types = {
        'application/rdf+xml',
    }
    params = {}
    prioritize_uri = True

