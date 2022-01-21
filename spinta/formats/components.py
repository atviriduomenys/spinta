class Format:
    content_type: str
    accept_types: set
    params: dict
    # TODO: Should be false, by default.
    streamable: bool = True
