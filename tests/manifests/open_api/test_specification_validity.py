"""The generated specification has to be a valid OpenAPI document.

Structural mistakes, a `$ref` pointing at nothing or a path parameter without a
placeholder among them, do not show up in a test looking at one field, but an
API gateway rejects the whole file over them.
"""

import pytest

from spinta.manifests.components import ManifestPath
from spinta.manifests.open_api.helpers import create_openapi_manifest
from spinta.manifests.open_api.udts_config import UdtsConfig
from tests.manifests.open_api.conftest import (
    MANIFEST,
    MANIFEST_WITH_COLLIDING_DATASETS,
    MANIFEST_WITH_COLLIDING_MODELS,
    MANIFEST_WITH_COLLIDING_OPERATION_IDS,
    MANIFEST_WITH_REF_SHAPES,
    MANIFEST_WITH_REFS,
    MANIFEST_WITH_SERVICES,
    MANIFEST_WITH_SOAP_PREPARE,
)

SERVICE_PATH = "datasets/gov/rc/jadis/at280/1"

UDTS_CONFIG = UdtsConfig(
    info={"title": "JADIS", "version": "1", "contact": {"email": "info@example.lt"}},
    servers=[
        {"url": "https://get.data.gov.lt", "description": "Production"},
        {"url": "https://test-get.data.gov.lt", "description": "Testing"},
    ],
    auth={"token_url": "https://get.data.gov.lt/auth/token"},
    external_docs={"url": "https://ivpk.github.io/uapi"},
)


def _assert_valid(open_api_spec: dict) -> None:
    validator = pytest.importorskip("openapi_spec_validator")

    # A `$ref` pointing at nothing raises here instead of being reported.
    errors = list(validator.OpenAPIV31SpecValidator(open_api_spec).iter_errors())

    assert errors == [], "\n".join(
        f"{'/'.join(str(part) for part in error.absolute_path)}: {error.message}" for error in errors
    )


@pytest.mark.parametrize(
    "manifest_data, kwargs",
    [
        # Whole manifest, as the Python API is called without a filter.
        (MANIFEST, {}),
        (MANIFEST_WITH_SOAP_PREPARE, {}),
        (MANIFEST_WITH_REFS, {}),
        # One data set, as the catalog calls it.
        (MANIFEST, {"main_dataset_name": "datasets/demo/system_data"}),
        (MANIFEST_WITH_REFS, {"main_dataset_name": "datasets/gov/cemetery"}),
        # One data service, as `spinta udts oas` exports it.
        (MANIFEST_WITH_SERVICES, {"service_path": SERVICE_PATH}),
        (MANIFEST_WITH_SERVICES, {"service_path": SERVICE_PATH, "config": UDTS_CONFIG}),
        (MANIFEST_WITH_SERVICES, {"service_path": "datasets/gov/rc/ntr/n249/1"}),
        # Names and reference shapes that need more than one schema.
        (MANIFEST_WITH_REF_SHAPES, {"service_path": SERVICE_PATH}),
        (MANIFEST_WITH_COLLIDING_DATASETS, {"service_path": SERVICE_PATH}),
        (MANIFEST_WITH_COLLIDING_MODELS, {"service_path": SERVICE_PATH}),
        (MANIFEST_WITH_COLLIDING_OPERATION_IDS, {"service_path": SERVICE_PATH}),
    ],
)
def test_generated_specification_is_valid(open_manifest_path_factory, manifest_data, kwargs):
    open_manifest_path = open_manifest_path_factory(manifest_data)

    _assert_valid(create_openapi_manifest(open_manifest_path, **kwargs))


def test_specification_of_the_example_configuration_is_valid(open_manifest_path: ManifestPath, tmp_path):
    """The configuration shipped for institutions has to produce a valid file."""
    from spinta.manifests.open_api import udts_config

    example = udts_config.__file__.replace("udts_config.py", "udts_cfg.example.yml")

    _assert_valid(
        create_openapi_manifest(
            open_manifest_path,
            service_path="datasets/demo/system_data",
            config=UdtsConfig.from_path(example),
        )
    )


@pytest.mark.parametrize(
    "manifest_data, kwargs",
    [
        (MANIFEST, {}),
        (MANIFEST, {"main_dataset_name": "datasets/demo/system_data"}),
        (MANIFEST_WITH_SERVICES, {"service_path": SERVICE_PATH, "config": UDTS_CONFIG}),
    ],
)
def test_every_security_requirement_names_a_declared_scheme(open_manifest_path_factory, manifest_data, kwargs):
    """A specification validator does not check this, an API gateway does."""
    open_manifest_path = open_manifest_path_factory(manifest_data)
    open_api_spec = create_openapi_manifest(open_manifest_path, **kwargs)

    declared = set(open_api_spec["components"]["securitySchemes"])
    requested = {
        scheme
        for operations in open_api_spec["paths"].values()
        for method, operation in operations.items()
        if method != "parameters" and isinstance(operation, dict)
        for requirement in operation.get("security", [])
        for scheme in requirement
    }

    assert requested
    assert requested <= declared, f"not declared: {sorted(requested - declared)}"
