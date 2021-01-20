from spinta.backends.components import Backend
from spinta.backends.components import BackendFeatures


class FileSystem(Backend):

    features = {
        BackendFeatures.WRITE,
    }
