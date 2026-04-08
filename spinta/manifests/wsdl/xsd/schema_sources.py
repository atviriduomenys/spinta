from __future__ import annotations

import pathlib
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import urlopen

import lxml.etree as etree
import lxml.objectify as objectify
from lxml.etree import _Element

from spinta import exceptions


TOLERATED_SCHEMA_ERRORS = (
    exceptions.ReferencedSchemaFileDoesNotExist,
    exceptions.MalformedReferencedSchema,
    exceptions.RemoteSchemaResourceUnavailable,
    exceptions.RemoteSchemaAuthenticationError,
    exceptions.RemoteSchemaServerError,
)


def is_remote_schema_source(path: str) -> bool:
    return path.startswith(("http://", "https://"))


def normalize_local_schema_path(path: str) -> str:
    if path.startswith("file://"):
        return path[len("file://"):]
    return path


def normalize_schema_source(path: str) -> str:
    if is_remote_schema_source(path):
        return path
    return str(pathlib.Path(normalize_local_schema_path(path)).resolve())


def resolve_schema_reference(base_source: str, schema_location: str) -> str:
    if is_remote_schema_source(schema_location):
        return schema_location

    if is_remote_schema_source(base_source):
        return urljoin(base_source, schema_location)

    location = normalize_local_schema_path(schema_location)
    base_path = pathlib.Path(normalize_local_schema_path(base_source))
    return str((base_path.parent / location).resolve())


def load_schema_root(
    source: str,
    *,
    urlopen_handler: Callable[[str], object] = urlopen,
) -> _Element:
    try:
        if is_remote_schema_source(source):
            document = etree.parse(urlopen_handler(source))
            objectify.deannotate(document, cleanup_namespaces=True)
            return document.getroot()

        path = normalize_local_schema_path(source)
        with open(path) as file:
            text = file.read()
            return etree.fromstring(bytes(text, encoding="utf-8"))
    except FileNotFoundError as error:
        raise exceptions.ReferencedSchemaFileDoesNotExist(path=normalize_schema_source(source)) from error
    except HTTPError as error:
        if error.code in {401, 403}:
            raise exceptions.RemoteSchemaAuthenticationError(
                path=source,
                status=error.code,
                error=error.reason,
            ) from error
        if error.code >= 500:
            raise exceptions.RemoteSchemaServerError(
                path=source,
                status=error.code,
                error=error.reason,
            ) from error
        raise exceptions.RemoteSchemaResourceUnavailable(
            path=source,
            error=f"HTTP status: {error.code}. {error.reason}",
        ) from error
    except URLError as error:
        raise exceptions.RemoteSchemaResourceUnavailable(
            path=source,
            error=str(error.reason),
        ) from error
    except (etree.XMLSyntaxError, etree.ParseError) as error:
        raise exceptions.MalformedReferencedSchema(
            path=normalize_schema_source(source),
            error=str(error),
        ) from error