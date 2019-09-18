import string


def format_error(message, kwargs):
    # TODO: deprecated and should be removed
    return Formatter().format(message, **kwargs)


class Formatter(string.Formatter):

    def get_field(self, field_name, args, kwargs):
        fields = field_name.split('.')
        first, fields = fields[0], fields[1:]
        obj = kwargs.get(first)
        for name in fields:
            if hasattr(obj, name):
                obj = getattr(obj, name)
            else:
                return '?', first
        return obj, first
