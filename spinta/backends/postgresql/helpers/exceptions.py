from spinta import commands, exceptions
import spinta.backends.postgresql.helpers.extractors as extractor
from spinta.components import DataItem, Action
from sqlalchemy.exc import IntegrityError, OperationalError
import psycopg2.errors as psy_errors

from spinta.exceptions import OutOfMemoryMigrate
from spinta.manifests.components import Manifest


@commands.create_exception.register(DataItem, IntegrityError)
def create_exception(item: DataItem, error: IntegrityError):
    return create_exception(item, error.orig)


@commands.create_exception.register(DataItem, psy_errors.ForeignKeyViolation)
def create_exception(item: DataItem, error: psy_errors.ForeignKeyViolation):
    error_message = error.diag.message_detail
    if item.action is Action.DELETE:
        error_model = extractor.extract_error_model(error_message)
        return exceptions.ReferringObjectFound(
            item.model.properties.get("_id"),
            model=error_model,
            id=item.saved.get("_id"))
    else:
        error_property_names = extractor.extract_error_property_names(error_message)
        error_ref_id = extractor.extract_error_ref_id(error_message)
        context = item.model
        if len(error_property_names) == 1:
            context = item.model.properties.get(error_property_names[0])
        return exceptions.ReferencedObjectNotFound(
            context,
            id=error_ref_id)


@commands.create_exception.register(DataItem, psy_errors.UniqueViolation)
def create_exception(item: DataItem, error: psy_errors.UniqueViolation):
    error_message = error.diag.message_detail
    error_property_names = extractor.extract_error_property_names(error_message)
    if len(error_property_names) == 1:
        error_property = item.model.properties.get(error_property_names[0])
        return exceptions.UniqueConstraint(error_property)
    else:
        return exceptions.CompositeUniqueConstraint(
            item.model,
            properties=",".join(error_property_names)
        )


@commands.create_exception.register(DataItem, Exception)
def create_exception(item: DataItem, error: Exception):
    raise error


@commands.create_exception.register(Manifest, OperationalError)
def create_exception(manifest: Manifest, error: OperationalError):
    return create_exception(manifest, error.orig)


@commands.create_exception.register(Manifest, psy_errors.OutOfMemory)
def create_exception(manifest: Manifest, error: psy_errors.OutOfMemory):
    return OutOfMemoryMigrate(manifest)


@commands.create_exception.register(Manifest, Exception)
def create_exception(manifest: Manifest, error: Exception):
    raise error
