from spinta.core.error import BaseError


class MissingParam(BaseError):
    template = "Missing parameter {param!r}."


class UnknownValue(BaseError):
    template = "Parameter {param!r}, can be one of {', '.join(map(str, choices))}."


class UnknownReference(BaseError):
    template = "Parameter {param!r} referrence {value!r} does not exist in {ref!r}."


class InvalidType(BaseError):
    template = "Parameter {param!r} must be of {expected!r} type."


class UnknownKeys(BaseError):
    template = "Parameter {param!r} has unknow keys: {', '.join(unknown)}."
