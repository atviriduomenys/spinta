"""UDTS data service path helpers.

A UDTS data service is identified by the leading part of a dataset path::

    datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
    └────────────── data service ──────────────┘ └── content ──┘

Everything after the data service path is the content of that service. One
OpenAPI specification describes exactly one data service with all of its data
sets.

Specification: https://ivpk.github.io/uapi/draft/#section/Architecture/API-URL-structure
"""

from __future__ import annotations

import re

from spinta.components import Model

SERVICE_PATH_ROOT = "datasets"

#: Service version is a single positive integer number.
version_re = re.compile(r"^[1-9][0-9]*$")

#: Number of segments in ``datasets/{form}/{org}/{is}/{service}``.
SERVICE_PATH_SEGMENTS = 5


def _is_version_segment(segment: str) -> bool:
    """UDTS service version is a single positive integer number.

    Specification describes it as a single positive integer number giving the
    major version of the service. The version segment is optional, in which case
    the latest service version is used.
    """
    return version_re.match(segment) is not None


def service_path_of(dataset_name: str) -> str | None:
    """Return the data service path a given data set belongs to.

    Returns `None` if the data set path does not reach the data service level.
    """
    segments = dataset_name.split("/")
    if len(segments) < SERVICE_PATH_SEGMENTS or segments[0] != SERVICE_PATH_ROOT:
        return None

    size = SERVICE_PATH_SEGMENTS
    if len(segments) > size and _is_version_segment(segments[size]):
        size += 1

    return "/".join(segments[:size])


def is_service_level_path(path: str) -> bool:
    """Check if given path is a UDTS data service path.

    Note that the version segment is a plain number, which `namespace_re` would
    reject, so name validation rules can not be reused here.
    """
    segments = path.split("/")
    if segments[0] != SERVICE_PATH_ROOT:
        return False
    if len(segments) == SERVICE_PATH_SEGMENTS:
        return True
    if len(segments) == SERVICE_PATH_SEGMENTS + 1:
        return _is_version_segment(segments[-1])
    return False


def find_services(dataset_names: list[str]) -> dict[str, list[str]]:
    """Group given data set names by the data service they belong to."""
    services: dict[str, list[str]] = {}
    for name in sorted(dataset_names):
        service = service_path_of(name)
        if service is not None:
            services.setdefault(service, []).append(name)
    return services


def is_under_service(name: str, service_path: str) -> bool:
    """Check if a data set belongs to a given data service.

    Matching is done on segment boundary, so `.../at280/1` does not match
    `.../at280/10`.
    """
    return name == service_path or name.startswith(f"{service_path}/")


def datasets_under_service(dataset_names: list[str], service_path: str) -> list[str]:
    """Data sets belonging to a given data service.

    A data service path selects exactly the data sets of that service, so an
    unversioned `.../at280` does not reach into `.../at280/1`, which is a data
    service of its own. A path of another shape, which is accepted with a
    warning, selects by prefix.
    """
    if is_service_level_path(service_path):
        return [name for name in dataset_names if service_path_of(name) == service_path]

    return [name for name in dataset_names if is_under_service(name, service_path)]


def relative_path(name: str, service_path: str) -> str:
    """Strip the data service path from a data set or model path."""
    if name == service_path:
        return ""
    if name.startswith(f"{service_path}/"):
        return name[len(service_path) + 1 :]
    return name


def service_schema_name(model: Model, service_path: str) -> str:
    """Build a component schema name for a model of a data service.

    Data sets of one service can hold models of the same name, so the data set
    path is kept as part of the schema name::

        datasets/gov/rc/jadis/at280/1/at280_israsas/DalyvioAsmensIsrasas
        -> at280_israsas_DalyvioAsmensIsrasas

    Path separators become underscores, so the result is not unique on its own;
    the names of one specification are made unique by `SchemaNamer`.
    """
    return relative_path(model.name, service_path).replace("/", "_")
