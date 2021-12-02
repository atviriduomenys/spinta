from spinta.components import Action


class Format:
    content_type: str
    accept_types: set
    params: dict
    # TODO: Should be false, by default.
    streamable: bool = True

    async def __call__(self, rows, action: Action, **params):
        raise NotImplementedError
