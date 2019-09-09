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
        super().__init__(self.message)


class UniqueConstraintError(DataError):
    def __init__(self, *, prop_name=None, model_name=None):
        self.message = f"{prop_name!r} is unique for {model_name!r} and a duplicate value is found in database."
        super().__init__(self.message)
