class _CommandsConfig:

    def __init__(self):
        self.modules = []
        self.pull = {}


class Config:

    def __init__(self):
        self.commands = _CommandsConfig()
        self.components = {}
        self.exporters = {}
        self.backends = {}
        self.manifests = {}
        self.ignore = []
        self.debug = False


class Store:

    def __init__(self):
        self.config = None
        self.backends = {}
        self.manifest = None


class Source:
    pass
