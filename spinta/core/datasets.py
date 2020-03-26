import enum

from spinta.components.models import Model


class Sector(enum.Enum):
    public = 'public'
    private = 'private'


def iterparams(model: Model):
    pass
