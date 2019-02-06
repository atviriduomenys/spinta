class Command:
    # Command name in {'name': ...} expression.
    name = None

    # List of types for which this command is applicable.
    types = ()

    # Specify a backend name if this command applies only to a specific
    # backend.
    backend = None

    def __init__(self, manifest, obj, backend, ns='default', stack: tuple = ()):
        self.manifest = manifest
        self.obj = obj
        self.backend = backend
        self.stack = stack
        self.ns = ns

    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, *args, **kwargs):
        kwargs.setdefault('ns', self.ns)
        kwargs.setdefault('stack', self.stack + (self,))
        return self.manifest.run(*args, **kwargs)

    def error(self, message):
        for cmd in reversed(self.stack):
            if hasattr(cmd.obj, 'path'):
                path = cmd.obj.path
                raise Exception(f"{path}: {message}")
        raise Exception(message)
