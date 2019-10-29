from typing import Optional, Any, Dict, Iterable, Tuple

import logging
import re


log = logging.getLogger(__name__)


class UnknownValue:

    def __str__(self):
        return '[UNKNOWN]'

    __repr__ = __str__


UNKNOWN_VALUE = UnknownValue()


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

    added = set()
    context = {}
    for k, path in schema.items():
        path = path or k
        name, *names = path.split('.')
        if name not in kwargs:
            continue
        added.add(name)
        value = kwargs
        for name in [name] + names:
            if name.endswith('()'):
                name = name[:-2]
                func = True
            else:
                func = False
            if isinstance(value, dict):
                value = value.get(name)
            elif hasattr(value, name):
                value = getattr(value, name)
            else:
                value = UNKNOWN_VALUE
                break
            if func:
                value = value()
        if value is not UNKNOWN_VALUE:
            context[k] = value

    for k in set(kwargs) - added:
        v = kwargs[k]
        if not isinstance(v, (int, float, str)):
            v = str(v)
        context[k] = v

    # Return sorted context.
    names = [
        'manifest',
        'schema',
        'backend',
        'dataset',
        'resource',
        'origin',
        'model',
        'property',
        'type',
    ]
    names += [x for x in schema if x not in names]
    names += [x for x in kwargs if x not in names]

    def sort_key(item: Tuple[str, Any]) -> Tuple[int, str]:
        key = item[0]
        try:
            return names.index(key), key
        except ValueError:
            return len(names), key

    return {k: v for k, v in sorted(context.items(), key=sort_key)}


def error_response(error):
    return {
        'type': error.type,
        'code': type(error).__name__,
        'template': error.template,
        'context': error.context,
        'message': error.message,
    }


def _render_template(error):
    if error.type in error.context:
        context = {
            **error.context,
            'this': f'<{error.type} name={error.context[error.type]!r}>',
        }
    else:
        context = error.context
    try:
        return error.template.format(**context)
    except KeyError:
        context = context.copy()
        template_vars_re = re.compile(r'\{(\w+)')
        for match in template_vars_re.finditer(error.template):
            name = match.group(1)
            if name not in context:
                context[name] = UNKNOWN_VALUE
        return error.template.format(**context)


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

        self.type = this.type if this else 'system'

        self.context = resolve_context_vars(self.context, this, kwargs)

        # Remove all unknown values.

        try:
            self.message = _render_template(self)
        except KeyError:
            log.exception("Can't render error message for %s.", self.__class__.__name__)
            self.message = self.template

        super().__init__(
            self.message + '\n' +
            ('  Context:\n' if self.context else '') +
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
                f' - {error.message}\n' +
                '     Context:\n' + ''.join(
                    f'       {k}: {v}\n' for k, v in error.context.items()
                )
                for error in self.errors
            ])
        )


class UserError(BaseError):
    status_code = 400


class ConflictingValue(UserError):
    status_code = 409
    template = "Conflicting value."
    context = {
        'given': None,
        'expected': None,
    }


class UniqueConstraint(UserError):
    template = "Given value already exists."


class InvalidOperandValue(UserError):
    template = "Invalid operand value for {operator!r} operator."


class UnknownOperator(UserError):
    template = "Unknown operator {operator!r}."


class ItemDoesNotExist(UserError):
    status_code = 404
    template = "Resource {_id!r} not found."


class ModelNotFound(UserError):
    status_code = 404
    template = "Model {model!r} not found."


class PropertyNotFound(UserError):
    status_code = 404
    template = "Property {property!r} not found."


class NoItemRevision(UserError):
    template = "'revision' must be given on rewrite operation."


class JSONError(UserError):
    template = "Invalid JSON."


class InvalidValue(UserError):
    template = "Invalid value."


class MultipleRowsFound(BaseError):
    template = "Multiple rows were found."


class MultipleDatasetModelsFoundError(BaseError):
    template = ("Found multiple {name!r} models in {dataset!r} "
                "dataset. Be more specific by providing resource name.")


class ManagedProperty(UserError):
    template = "Value of this property is managed automatically and cannot be set manually."


class NotFoundError(BaseError):
    template = "No results where found."
    status_code = 500


class NodeNotFound(UserError):
    template = "Node {name!r} of type {type!r} not found."


class ModelReferenceNotFound(BaseError):
    template = "Model reference {ref!r} not found."


class SourceNotSet(UserError):
    status_code = 404
    template = (
        "Dataset {dataset!r} resource {resource!r} source is not set. "
        "Make sure {resource!r} name parameter in {path} or environment variable {envvar} is set."
    )


class InvalidManifestFile(BaseError):
    template = "Error while parsing {filename!r} file: {error}"


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


class FileNotFound(UserError):
    template = "File {file!r} not found."


class UnknownModelReference(BaseError):
    template = "Unknown model reference given in {param!r} parameter."


class InvalidDependencyValue(BaseError):
    template = "Dependency must be in 'object/name.property' form, got: {dependency!r}."


class MultipleModelsInDependencies(BaseError):
    template = "Dependencies are allowed only from single model, but more than one model found: {models}."


class MultipleCommandCallsInDependencies(BaseError):
    template = "Only one command call is allowed."


class MultipleCallsOrModelsInDependencies(BaseError):
    template = "Only one command call or one model is allowed in dependencies."


class InvalidSource(BaseError):
    template = "Invalid source. {error}"
    context = {
        'source': 'source.type',
    }


class UnknownParameter(BaseError):
    template = "Unknown parameter {parameter!r}."
    context = {
        'parameter': 'param',
    }


class FieldNotInResource(UserError):
    template = "Unknown property {property!r}."
    context = {
        'property': 'prop',
    }


class UnknownContentType(UserError):
    status_code = 415
    template = "Unknown content type {content_type!r}."


class UnknownAction(UserError):
    template = "Unknown action {action!r}."


class UnknownOutputFormat(UserError):
    template = "Unknown output format {name}."


class UnknownRequestParameter(UserError):
    template = "Unknown request parameter {name!r}."


class OutOfScope(UserError):
    template = "{this} is out of given scope {scope!r}."
    context = {
        'scope': 'scope.name',
    }


class UnhandledException(BaseError):
    status_code = 500
    template = "Unhandled exception {exception}: {error}."
    context = {
        'exception': 'error.__class__.__name__',
    }


class NewItemHasIdAlready(UserError):
    template = "New item has id already set"


class NewItemHasRevisionAlready(UserError):
    template = "New item has revision already set"
