from spinta.formats.components import Format


class Xlsx(Format):
    content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    accept_types = {
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    }
    params = {}
