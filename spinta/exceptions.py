class BaseError(Exception):
    template = ""
    typ = None
    status_code = 400

    def __init__(self, **kwargs):
        self.context = kwargs
        self.message = self.template.format(**self.context)
        super().__init__(self.message)

    def error(self):
        error = {
            "code": type(self).__name__,
            "type": self.typ,
            "template": self.template,
            "message": self.message,
            "context": self.context,
        }
        return error



# XXX: should we rename this as it clashes with python builtin `SystemError`?
class SystemError(BaseError):
    typ = "system"


class ModelError(BaseError):
    typ = "model"

    def __init__(self, **kwargs):
        # model must be given as kwarg
        self.model = kwargs["model"]
        super().__init__(**kwargs)

    def error(self):
        base_error = super().error()
        base_error["model"] = self.model
        return base_error


class PropertyError(BaseError):
    typ = "property"

    def __init__(self, **kwargs):
        # model and prop must be given as kwargs
        # FIXME: client facing context should have `property` instead of `prop`.
        self.model = kwargs["model"]
        self.prop = kwargs["prop"]
        super().__init__(**kwargs)

    def error(self):
        base_error = super().error()
        base_error["model"] = self.model
        base_error["prop"] = self.prop
        return base_error


class PropertyTypeError(PropertyError):
    # FIXME: change type_name to type
    template = 'Field {prop!r} should receive value of {type_name!r} type.'

    def __init__(self, type_):
        super().__init__(model=type_.prop.model.name,
                         prop=type_.prop.place,
                         type_name=type_.name,)


class ModelPropertyValueConflictError(PropertyError):
    template = ' '.join((
        "Property value must match database.",
        "Given value: {given_value!r}, existing value: {existing_value!r}."
    ))
    status_code = 409


class UnknownObjectPropertiesError(PropertyError):
    template = "Object does not contain given properties: {props_list}"


class UniqueConstraintError(PropertyError):
    template = "{prop!r} is unique for {model!r} and a duplicate value is found in database."


class FileNotFoundInResourceError(PropertyError):
    template = "File {prop!r} not found in {id_!r}."
    status_code = 404


class SearchOperatorTypeError(PropertyError):
    # FIXME: change type_name to type and operator_name to operator
    template = "Operator {operator_name!r} received value for {prop!r} of type {type_name!r}."


class SearchStringOperatorError(PropertyError):
    # FIXME: change type_name to type and operator_name to operator
    template = ' '.join((
        "Operator {operator_name!r} requires string.",
        "Received value for {prop!r} is of type {type_name!r}."
    ))


class FileDoesNotExistError(PropertyError):
    template = "File {path!r} does not exist."


# FIXME: this error is very similar to UnknownModelPropertiesErroro
# maybe it's possible to combine them
class ModelPropertyError(ModelError):
    template = "Model {model!r} does not contain field {query_param!r}."


class UnknownModelPropertiesError(ModelError):
    template = "Model does not contain given properties: {props_list}"


class ResourceNotFoundError(ModelError):
    template = "Resource {model!r} with id {id_!r} does not exist."
    status_code = 404


class ModelNotFoundError(ModelError):
    template = "Model {model!r} not found."
    status_code = 404


class MissingRevisionOnRewriteError(ModelError):
    template = "'revision' must be given on rewrite operation."


# FIXME: Probably it would be useful to also include original error
# from JSON parser, to tell user what exactly is wrong with given JSON.
class JSONError(SystemError):
    template = "Not a valid json"


class IDInvalidError(SystemError):
    template = "ID value is not valid"


class DateTypeError(SystemError):
    template = "Invalid isoformat date value: {date!r}"


class DateTimeTypeError(SystemError):
    template = "Invalid isoformat datetime value: {date!r}"


class ArrayTypeError(SystemError):
    template = "Invalid array value: {value!r}"


class ObjectTypeError(SystemError):
    template = "Invalid object value: {value!r}"


class MultipleRowsFound(SystemError):
    template = "Multiple rows were found."


class MultipleDatasetModelsFoundError(SystemError):
    template = ("Found multiple {name!r} models in {dataset!r} "
                "dataset. Be more specific by providing resource name.")


class RevisionError(SystemError):
    template = "Client cannot create 'revision'. It is set automatically"


class NotFoundError(SystemError):
    template = "No results where found."
    status_code = 404


class DatasetNotFoundError(SystemError):
    # FIXME: change dataset_name to dataset
    template = "Dataset ':dataset/{dataset_name}' not found."
    status_code = 404


class DatasetResourceNotFoundError(SystemError):
    # FIXME: change dataset_name to dataset and resource_name to resource
    template = "Resource ':dataset/{dataset_name}/:resource/{resource_name}' not found."
    status_code = 404
