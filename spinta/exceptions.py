from typing import Optional, Any, Dict, Iterable

import logging


log = logging.getLogger(__name__)


def resolve_context_vars(schema: Dict[str, str], this: Optional[Any], kwargs: dict):
    """Resolve value from given kwargs and schema."""
    # Extend context by calling get_error_context command on first positional
    # argument of Node or Type type.
    if this:
        from spinta import commands
        schema = {
            **commands.get_error_context(this),
            **schema,
        }
        kwargs = {**kwargs, 'this': this}

    context = {}
    known = set()
    for k, path in schema.items():
        path = path or k
        name, *names = path.split('.')
        known.add(name)
        value = kwargs
        for name in [name] + names:
            if name.endswith('()'):
                name = name[:-2]
                func = True
            else:
                func = False
            if isinstance(value, dict):
                value = kwargs.get(name)
            elif hasattr(value, name):
                value = getattr(value, name)
            else:
                value = '[UNKNOWN]'
                break
            if func:
                value = value()
        context[k] = value

    # Check if unknown and missing keyword arguments, but only log the situation
    # without breaking original error.
    given = set(kwargs)
    unknown = given - known
    if unknown:
        unknown = ', '.join(map(repr, sorted(unknown)))
        log.error("Unknown BaseError kwargs: %s.", unknown, stack_info=True)
    missing = known - given
    if missing:
        missing = ', '.join(map(repr, sorted(missing)))
        log.error("Missing BaseError kwargs: %s.", missing, stack_info=True)

    return context


def error_response(error):
    return {
        'type': error.type,
        'code': type(error).__name__,
        'template': error.template,
        'context': error.context,
        'message': error.message,
    }


class BaseError(Exception):
    type = None
    status_code = 500
    template = None
    context = {}

    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            this = None
        elif len(args) == 1:
            this = args[0]
        else:
            this = None
            log.error("Only one positional argument is alowed, but %d was given.", len(args), stack_info=True)

        from spinta.components import Node
        from spinta.types.datatype import DataType
        self.type = 'system'
        if this:
            if isinstance(this, Node):
                self.type = this.type
            elif isinstance(this, DataType):
                self.type = this.prop.type

        self.context = resolve_context_vars(self.context, this, kwargs)
        try:
            self.message = self.template.format(**self.context)
        except KeyError:
            log.exception("Can't render error message for %s.", self.__class__.__name__)
            self.message = self.template

        super().__init__(
            self.message + '\n' +
            '  Context:\n' +
            ''.join(
                f'    {k}: {v}\n'
                for k, v in self.context.items()
            )
        )


class MultipleErrors(Exception):

    def __init__(self, errors: Iterable[BaseError]):
        self.errors = list(errors)
        super().__init__(
            'Multiple errors:\n' + ''.join([
                ' - {error.message}\n' +
                '     Context:\n' + ''.join(
                    f'       {k}: {v}\n' for k, v in error.context.items()
                )
                for error in self.errors
            ])
        )


class ConflictingValue(BaseError):
    status_code = 409
    template = "Conflicting value."
    context = {
        'given': None,
        'expected': None,
    }


class UniqueConstraint(BaseError):
    status_code = 400
    template = "Given value already exists."


class InvalidOperandValue(BaseError):
    status_code = 400
    template = "Invalid operand value for {operator!r} operator."
    context = {
        'operator': None,
    }


class ResourceNotFound(BaseError):
    status_code = 404
    template = "Resource {id!r} not found."
    context = {
        'id': None,
    }


class ModelNotFound(BaseError):
    status_code = 404
    template = "Model {model!r} not found."
    context = {
        'model': None,
    }


class MissingRevisionOnRewriteError(BaseError):
    status_code = 400
    template = "'revision' must be given on rewrite operation."


# FIXME: Probably it would be useful to also include original error
# from JSON parser, to tell user what exactly is wrong with given JSON.
class JSONError(BaseError):
    status_code = 400
    template = "Not a valid json"
    context = {
        'error': None,
    }


class InvalidValue(BaseError):
    status_code = 400
    template = "Invalid value."


class MultipleRowsFound(BaseError):
    template = "Multiple rows were found."


class MultipleDatasetModelsFoundError(BaseError):
    template = ("Found multiple {name!r} models in {dataset!r} "
                "dataset. Be more specific by providing resource name.")


class ManagedProperty(BaseError):
    status_code = 400
    template = "Value of this property is managed automatically and cannot be set manually."
    context = {
        'property': None,
    }


class NotFoundError(BaseError):
    template = "No results where found."
    status_code = 500


class NodeNotFound(BaseError):
    template = "Node {name!r} of type {type!r} not found."
    context = {
        'type': None,
        'name': None,
    }


class ModelReferenceNotFound(BaseError):
    template = "Model reference {ref!r} not found."
    context = {
        'ref': None,
    }


class SourceNotSet(BaseError):
    template = (
        "Dataset {dataset!r} resource {resource!r} source is not set. "
        "Make sure {resource!r} name parameter in {path} or environment variable {envvar} is set."
    )
    status_code = 404


class InvalidManifestFile(BaseError):
    template = "Error while parsing {filename!r} file: {error}"
    context = {
        'manifest': None,  # Manifest name.
        'filename': None,
        'error': None,  # Error message indicating why manifest file is invalid.
    }


class UnknownProjectOwner(BaseError):
    template = "Unknown owner {owner}."
    context = {
        'owner': 'this.owner'
    }


class UnknownProjectDataset(BaseError):
    template = "Unknown project dataset."
    context = {
        'manifest': 'project.parent.name',
        'dataset': 'project.dataset',
        'project': 'project.name',
        'filename': 'project.path',
    }


class MissingRequiredProperty(BaseError):
    template = "Property {property!r} is required."
    context = {
        'property': 'prop',
    }


class FileNotFound(BaseError):
    status_code = 400
    template = "File {file!r} not found."
    context = {
        'file': None,
    }


class UnknownModelReference(BaseError):
    template = "Unknown model reference given in {param!r} parameter."
    context = {
        'param': None,
        'reference': None,
    }


class InvalidDependencyValue(BaseError):
    template = "Dependency must be in 'object/name.property' form, got: {dependency!r}."
    context = {
        'dependency': None,
    }


class MultipleModelsInDependencies(BaseError):
    template = "Dependencies are allowed only from single model, but more than one model found: {models}."
    context = {
        'models': None,
    }


class MultipleCommandCallsInDependencies(BaseError):
    template = "Only one command call is allowed."


class MultipleCallsOrModelsInDependencies(BaseError):
    template = "Only one command call or one model is allowed in dependencies."


class InvalidSource(BaseError):
    template = "Invalid source. {error}"
    context = {
        'source': 'source.type',
        'error': None,
    }


class UnknownParameter(BaseError):
    template = "Unknown parameter {parameter!r}."
    context = {
        'parameter': 'param',
    }


class UnknownProperty(BaseError):
    status_code = 400
    template = "Unknown property {property!r}."
    context = {
        'property': None,
    }
