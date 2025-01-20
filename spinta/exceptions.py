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
    if this:
        context['component'] = type(this).__module__ + '.' + type(this).__name__
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
        'component',
        'manifest',
        'schema',
        'backend',
        'dataset',
        'resource',
        'model',
        'entity',
        'property',
        'attribute',
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


class BaseError(Exception):
    type: str = None
    status_code: int = 500
    template: str = None
    headers: Dict[str, str] = {}
    context: Dict[str, Any] = {}

    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            this = None
        elif len(args) == 1:
            this = args[0]
        else:
            this = None
            log.error("Only one positional argument is alowed, but %d was given.", len(args), stack_info=True)

        self.type = this.type if this and hasattr(this, 'type') else 'system'

        self.context = resolve_context_vars(self.context, this, kwargs)

    def __str__(self):
        return (
            self.message + '\n' +
            ('  Context:\n' if self.context else '') +
            ''.join(
                f'    {k}: {v}\n'
                for k, v in self.context.items()
            )
        )

    @property
    def message(self):
        try:
            return _render_template(self)
        except KeyError:
            log.exception("Can't render error message for %s.", self.__class__.__name__)
            return self.template


def error_response(error: BaseError):
    return {
        'type': error.type,
        'code': type(error).__name__,
        'template': error.template,
        'context': error.context,
        'message': error.message,
    }


def _render_template(error: BaseError):
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


class UpgradeError(BaseError):
    status_code = 500


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
    template = "Resource {id!r} not found."


class DatasetNotFound(UserError):
    status_code = 404
    template = "Dataset {dataset!r} not found."


class NamespaceNotFound(UserError):
    status_code = 404
    template = "Namespace {namespace!r} not found."


class ModelNotFound(UserError):
    status_code = 404
    template = "Model {model!r} not found."


class PropertyNotFound(UserError):
    status_code = 404
    template = "Property {property!r} not found."


class NoItemRevision(UserError):
    template = "'_revision' must be given on rewrite operation."


class JSONError(UserError):
    template = "Invalid JSON."


class InvalidValue(UserError):
    template = "Invalid value."


class InvalidPropertyType(UserError):
    template = "Invalid property type, expected {expected}, got {type}.."


class ValueNotInEnum(UserError):
    template = "Given value {value} is not defined in enum."


class UndefinedEnum(UserError):
    template = "Enum {name!r} is not defined."


class EmptyStringSearch(UserError):
    template = \
        "Empty string can't be used with `{op}`. " \
        "Use `exact` parameter."


class InvalidToken(UserError):
    status_code = 401
    template = "Invalid token"
    headers = {'WWW-Authenticate': 'Bearer error="invalid_token"'}


class BasicAuthRequired(UserError):
    status_code = 401
    template = "Unauthorized"
    headers = {'WWW-Authenticate': 'Basic realm="Authentication required."'}


class AuthorizedClientsOnly(UserError):
    status_code = 401
    template = "This resource can only be accessed by an authorized client."


class MultipleRowsFound(BaseError):
    template = "Multiple rows were found."


class ManagedProperty(UserError):
    template = "Value of this property is managed automatically and cannot be set manually."


class InvalidManagedPropertyName(UserError):
    template = "Invalid managed property name. Expected name {name!r}, got {property!r}."


class NotFoundError(BaseError):
    template = "No results where found."
    status_code = 500


class NodeNotFound(UserError):
    template = "Node {name!r} of type {type!r} not found."


class ModelReferenceNotFound(BaseError):
    template = "Model reference {ref!r} not found."


class ModelReferenceKeyNotFound(BaseError):
    template = "Model reference key {ref!r} not found in {model!r}."


class SourceNotSet(UserError):
    status_code = 404
    template = (
        "Dataset {dataset!r} resource {resource!r} source is not set. "
        "Make sure {resource!r} name parameter in {path} or environment variable {envvar} is set."
    )


class InvalidManifestFile(BaseError):
    template = "Error while parsing {eid!r} manifest entry: {error}"


class CoordinatesOutOfRange(UserError):
    template = 'Given coordinates: {given!r} ar not within the `EPSG: {srid!r}` available bounds: {bounds} (west, south, east, north).'


class ManifestFileDoesNotExist(BaseError):
    template = "Manifest file {path} does not exist."


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
    template = "Property {prop!r} is required."


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


class InvalidUuidValue(UserError):
    template = "Invalid uuid value: {value}."


class InvalidRefValue(UserError):
    template = "Invalid reference value: {value}."


class InvalidLevel(UserError):
    template = "Invalid level value \"{level}\"."


class InvalidParameterValue(BaseError):
    template = "Invalid parameter {parameter!r} value."


class TooManyParameters(BaseError):
    template = (
        "Too many parameters, you can only give up to {max_params} parameters."
    )


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
    template = "Unknown output format {name!r}."


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


class ModelSourceNotFound(UserError):
    template = "Model source {table!r} not found."


class UnavailableSubresource(UserError):
    template = "Subresources only of type Object and File are accessible"


class InvalidPostUrl(UserError):
    template = "Invalid URL for POST request"


class MultipleParentsError(Exception):
    pass


class UnknownPropertyType(UserError):
    template = "Unknown property type {type!r}."


class UnknownMethod(UserError):
    template = "Unknown method {name!r} with args {expr}."


class MissingReference(UserError):
    template = "Missing reference {ref!r} referenced from {param!r} parameter."


class UnacceptableFileName(UserError):
    template = "Path is not acceptable in filename {file!r}"


class NoAuthServer(UserError):
    template = "Authorization server is disabled, use an external authorization server."


class NoTokenValidationKey(UserError):
    template = "A token validation key is required, set it via token_validation_key configuration parameter."


class NoExternalName(UserError):
    template = "Property {property} does not have 'external' name."


class NoKeyMap(UserError):
    template = "Key map is not configured."


class UnknownKeyMap(UserError):
    template = "Keymap {keymap!r} is not found."


class BackendNotFound(UserError):
    template = "Can't find backend {name!r}."


class NoBackendConfigured(UserError):
    template = "Backend is not configured, can't proceed the request."


class UnexpectedFormulaResult(UserError):
    template = (
        "Unexpected formula {formula} result. "
        "Expected {expected}, instead got a {received}."
    )


class FormulaError(UserError):
    template = "Error while interpreting formula {formula}: {error}."


class UnknownBind(FormulaError):
    template = "Unknown bind {name!r}."


class RequiredConfigParam(UserError):
    template = "Configuration parameter {name!r} is required."


class IncompatibleForeignProperties(UserError):
    template = (
        "Can't join {this} and {right}, these two properties does not have "
        "direct connection with one another."
    )


class KeymapNotSet(UserError):
    template = (
        "Keymap is required for {this}, but is not configured. Please make "
        "sure a key map is configured."
    )


class NoRefPropertyForDenormProperty(UserError):
    template = (
        "Property {ref!r} with type 'ref' or 'object' must be defined "
        "before defining property {prop!r}."
    )


class ReferencedPropertyNotFound(PropertyNotFound):
    pass


class RemoteClientError(UserError):
    pass


class RemoteClientCredentialsNotFound(RemoteClientError):
    template = (
        "Remote client credentials not found for {name!r}. Make sure, a "
        "section [{section}] exists in {credentials} file. You can use "
        "`spinta remote add {name}` command to add it."
    )


class RemoteClientCredentialsNotGiven(RemoteClientError):
    template = (
        "Make sure client name and secret is given in {credentials} file, "
        "[{section}] section."
    )


class RemoteClientScopesNotGiven(RemoteClientError):
    template = (
        "Make sure at least one scope is given for [{section}] in "
        "{credentials} file."
    )


class DupicateProperty(UserError):
    template = "Duplicate property {name}."


class RequiredProperty(UserError):
    template = "Property is required."


class UnableToCast(UserError):
    template = "Unable to cast {value} to {type} type."


class NotImplementedFeature(BaseError):
    template = "{feature} is not implemented yet."


class ReferencedObjectNotFound(UserError):
    template = "Referenced object {id!r} not found."


class ReferringObjectFound(UserError):
    template = "Object {id!r} is still being referenced in table {model!r}."


class CompositeUniqueConstraint(UserError):
    template = "Given values for composition of properties ({properties}) already exist."


class SourceCannotBeList(BaseError):
    template = "Source can't be a list, use prepare instead."


class InsufficientPermission(UserError):
    status_code = 403
    template = "You need to have {scope!r} in order to access this API endpoint."


class InsufficientPermissionForUpdate(UserError):
    status_code = 403
    template = "You do not have a permission to update '{field}' field."


class UnknownPropertyInRequest(UserError):
    template = "Property '{property}' is not part of allowed properties: '{properties}'"


class ClientWithNameAlreadyExists(UserError):
    template = "Client with name '{client_name}' already exists."


class ClientAlreadyExists(UserError):
    template = "Client '{client_id}' already exists."


class EmptyPassword(UserError):
    template = "Client password cannot be empty."


class UnknownRequestQuery(UserError):
    template = "Request '{request}' does not support '{query}' query."


class InvalidRequestQuery(UserError):
    template = "Query '{query}' requires '{format}' format."


class UnexpectedFormKeys(UserError):
    template = "Unexpected keys: {unknown_keys}, only keys that are allowed are: {allowed_keys}."


class RequiredFormKeyWithCondition(UserError):
    template = "'{key}' key is required when {condition}."


class InvalidFormKeyCombination(UserError):
    template = "Form only accepts one of the keys: {keys}."


class MissingFormKeys(UserError):
    template = "Form requires to have at least one of the keys: {keys}."


class InvalidInputData(UserError):
    template = "'{key}' does not accept value: '{given}', {condition}"


class InvalidResourceSource(UserError):
    template = "'{source}' is unacceptable resource source, it must be URL."


class UnknownManifestType(BaseError):
    template = "Can't find manifest component matching given type {type!r}."


class UnknownManifestTypeFromPath(BaseError):
    template = "Can't find manifest type matching given path {path!r}."


class InvalidPageParameterCount(UserError):
    template = "Parameter 'page' only accepts one of page key, size, or disable attributes."


class InvalidPushWithPageParameterCount(UserError):
    template = "Given push page property count does not match model required properties: {properties}."


class InvalidPageKey(UserError):
    template = "Given '{key}' page key is invalid."


class InfiniteLoopWithPagination(UserError):
    template = '''
    Pagination values has cause infinite loop while fetching data.
    Page of size: {page_size}, first value is the same as previous page's last value, which is:
    {page_values}
    '''


class TooShortPageSize(UserError):
    template = '''
    Page of size: {page_size} is too small, some duplicate values do not fit in a single page.
    Which can cause either loss of data, or cause infinite loop while paginating.
    Affected row: {page_values}
    
    To fix this, please either increase page size in the manifest, or 'default_page_size' value in the configs.
    Alternatively make page's structure more complex, by adding more properties to it.
    
    When migrating from older versions to newer versions of spinta you might get this error if push state
    database is out of sync, add '--sync' tag to 'push' command to synchronize it.
    '''


class TooShortPageSizeKeyRepetition(TooShortPageSize):

    def __init__(self, *args, **kwargs):
        super(TooShortPageSize, self).__init__(*args, **kwargs)
        self.template = f'''
        {self.template}
        Error has been triggered because:
        New page's key has been encountered multiple times in the previous page and it is the same for the future value.
        This will cause the same data to be fetched multiple times.
        '''


class DuplicateRowWhilePaginating(BaseError):
    template = "Encountered a duplicate row with page key: '{key}'"


class UnauthorizedPropertyPush(UserError):
    code = 403
    template = "You do not have permission to push this property."


class InvalidArgumentInExpression(BaseError):
    template = "Invalid {arguments} arguments given to {expr} expression."


class BackendNotGiven(UserError):
    template = "Model is operating in external mode, yet it does not have assigned backend to it."


class UnauthorizedKeymapSync(UserError):
    code = 403
    template = "You do not have permission to sync this model's keymap."


class GivenValueCountMissmatch(BaseError):
    template = '''
    While assigning ref values {given_count} were given, but {expected_count} were expected.
    This can happen, when there are no primary keys set on ref's model, or the keys can be nullable.
    '''


class PartialTypeNotFound(BaseError):
    template = "Partial type can only be used for ref type."


class NoReferencesFound(UserError):
    template = "Property {prop_name!r} of type 'Ref' was not found."


class MultipleBackRefReferencesFound(UserError):
    template = "Model {model!r} contains multiple references to backref, please specify which one to use."


class NoBackRefReferencesFound(UserError):
    template = "Model {model!r} does not contain any suitable properties for backref."


class InvalidBackRefReferenceAmount(UserError):
    template = "Backref {backref!r} can only reference one property."


class CannotModifyBackRefProp(UserError):
    template = "It is impossible to directly set or modify Backref property."


class OneToManyBackRefNotSupported(UserError):
    template = "One to many relationship is not supported by Backref."


class SummaryWithMultipleProperties(UserError):
    template = "Summary with multiple properties is not supported."


class TooManyModelUriProperties(UserError):
    template = "Model already has {uri_prop!r} set as uri property."


class DataTypeCannotBeUsedForNesting(UserError):
    template = "Type {dtype!r} cannot be used for nesting properties."


class NestedDataTypeMismatch(UserError):
    template = "While nesting, {initial!r} type cannot be cast to {required!r} type."


class LangNotDeclared(UserError):
    template = "Language {lang!r} has not been declared."


class TooManyLangsGiven(UserError):
    template = "Too many languages given in 'content-language' header, expected only one, but were given {amount}."


class UnableToDetermineRequiredLang(UserError):
    template = "Unable to determine required language."


class CannotSelectTextAndSpecifiedLang(UserError):
    template = "Cannot select undisclosed language Text at the same time when disclosed language Text is selected."


class DuplicateRdfPrefixMissmatch(UserError):
    template = "Currently system does not support prefix missmatch. Prefix {prefix!r} has {old_value!r} and {new_value!r} values given."


class InvalidName(UserError):
    template = 'Invalid {name!r} {type} code name.'


class NoneValueComparison(UserError):
    template = "None values can only be compared using 'eq' or 'ne' operands, {op!r} was given."


class InvalidDenormProperty(UserError):
    template = 'Cannot create Denorm property {denorm!r}, because it is part of {ref!r} refprops: {refprops}.'


class RefPropTypeMissmatch(UserError):
    template = 'Refprop {refprop!r} requires {required_type!r} type, but was given {given_type!r}.'


class InheritPropertyValueMissmatch(UserError):
    template = 'Expected {expected!r} value, but got {given!r}.'


class OutOfMemoryMigrate(UserError):
    template = "Ran out of shared memory while migrating. Use 'spinta migrate --autocommit' flag, or increase 'max_locks_per_transaction'."


class ManifestObjectNotDefined(UserError):
    template = "Object {obj!r} is not defined in manifest objects list."


class InvalidIdType(UserError):
    template = "Id {id!r} of {id_type!r} type is invalid."


class NotSupportedManifestType(UserError):
    template = "This feature is only supported with {supported_type} manifests, but {manifest_name!r} is a {given_type} manifest."


class InvalidSchemaUrlPath(UserError):
    template = "Cannot directly alter model or namespace schema. Only datasets are allowed."


class ModifyOneDatasetSchema(UserError):
    template = "Schema modification requires 1 dataset to be given, but {given_amount} were given."


class DatasetNameMissmatch(UserError):
    template = "Expected {expected_dataset!r} dataset, but were given {given_dataset!r}."


class DatasetSchemaRequiresIds(UserError):
    template = "All given schema rows require UUID to be set in the id field."


class ModifySchemaRequiresFile(UserError):
    template = "To modify dataset's schema, you are required to attach csv type manifest."


class FileSizeTooLarge(UserError):
    template = "Given file is too large, only up to {allowed_amount} {measure} is allowed."


class SRIDNotSetForGeometry(BaseError):
    template = "Geometry SRID is required, but was given None."


class KeyNotFound(UserError):
    template = "{key!r} key is not in given data dictionary keys: {dict_keys!r}."


class InvalidParamSource(UserError):
    template = "Unable to recognize {param!r} param's source {source!r} type, given: {given_type!}, expected: {expected_types!r}."


class MigrateScalarToRefTooManyKeys(UserError):
    template = '''
    Migration between scalar types and Ref type is only supported when targeted Ref's model contains
    only 1 primary key (except Ref level 3 to scalar), but were given: {primary_keys}
    '''


class MigrateScalarToRefTypeMissmatch(UserError):
    template = '''
    Migration between scalar types and Ref requires, that mapped columns match their types.
    {details}
    '''


class KeyMapGivenKeyMissmatch(UserError):
    template = '''
    The encoding for the {name!r} keymap already includes the key {found_key!r}, associated with the value {value!r}. 
    You attempted to assign the new primary key {given_key!r}, which conflicts with the existing key. 
    Make sure that all keymap values are unique.
    '''


class MultiplePrimaryKeyCandidatesFound(UserError):
    template = '''
    While assigning foreign key for non-primary key `Ref` property, using values: {values!r}
    found multiple possible matches. This can occur when non-primary keys are not unique and there are duplicate values.
    '''


class NoPrimaryKeyCandidatesFound(UserError):
    template = '''
    While assigning foreign key for non-primary key `Ref` property, using values: {values!r}
    no possible matches were found. This can occur when trying to assign values that do not exist in foreign table.
    '''


class ClientsMigrationRequired(UpgradeError):
    template = '''
    Clients folder structure is out of date. Please migrate it using:
    'spinta upgrade', or 'spinta upgrade -r clients' commands.
    
    Old structure used to be:
    ../clients/???.yml
    
    New structure is:
    ../clients/helpers/keymap.yml
    ../clients/id/??/??/???.yml
    
    Where `keymap.yml` stores `client_name` and `client_id` mapping.
    '''


class ClientsKeymapNotFound(UpgradeError):
    template = '''
    Cannot find `../clients/helpers/keymap.yml` file.
    
    Make sure it exists.
    Consider running `spinta upgrade` or `spinta upgrade -r clients` commands 
    '''


class ClientsIdFolderNotFound(UpgradeError):
    template = '''
    Cannot find `../clients/id` folder.

    Make sure it exists.
    Consider running `spinta upgrade` or `spinta upgrade -r clients` commands 
    '''


class InvalidClientsKeymapStructure(UpgradeError):
    template = '''
    Could not load Clients `keymap.yml`.
    Structure is invalid.

    Fix it or consider running `spinta upgrade -f -r clients` command.
    '''


class UpgradeScriptNotFound(UserError):
    template = '''
    Upgrade script {script!r} not found.
    Available scripts: {available_scripts}.
    '''


class InvalidScopes(UserError):
    template = "Request contains invalid, unknown or malformed scopes: {scopes}."


class DirectRefValueUnassignment(UserError):
    template = '''
    Cannot directly set ref's _id value to None.
    You have to set ref to None, which will also remove all additional stored values related to that ref (child properties).
    '''


class BackendUnavailable(BaseError):
    template = '''
    Unable to access {name!r} backend, please try again later.
    '''


class InvalidClientFileFormat(UserError):
    template = "File {client_file} data must be a dictionary, not a {client_file_type}."


class MissingRefModel(UserError):
    template = 'Property "{property_name}" of type "{property_type}" in the model "{model_name}" should have a model name in the `ref` column, to which it refers.'
