import pathlib

from spinta.backends.components import Backend
from spinta.backends.constants import BackendFeatures


class FileSystem(Backend):

    features = {
        BackendFeatures.WRITE,
    }

    path: pathlib.Path
