class NotFound(Exception):
    pass


class FoundMultiple(Exception):
    pass


class DataError(Exception):
    pass


class ConflictError(Exception):
    pass


class RevisionException(DataError):
    def __init__(self):
        self.message = "Client cannot create 'revision'. It is set automatically"
