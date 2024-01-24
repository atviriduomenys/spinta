import os
import tempfile

from starlette.requests import Request, FormData
from starlette.datastructures import UploadFile

from typing import Tuple

from spinta import commands
from spinta.auth import check_scope
from spinta.components import Context, UrlParams
from spinta.datasets.inspect.helpers import create_manifest_from_inspect
from spinta.exceptions import UnexpectedFormKeys, InvalidFormKeyCombination, RequiredFormKeyWithCondition, \
    MissingFormKeys, InvalidInputData
from spinta.manifests.components import ManifestPath, Manifest


def _validate_form_data(form: FormData):
    # Check form key boundary
    allowed_values = [
        'dataset',
        'manifest.type',
        'manifest.file',
        'manifest.source',
        'resource.type',
        'resource.file',
        'resource.source',
        'resource.prepare'
    ]

    if not set(form.keys()).issubset(allowed_values):
        unknown_keys = list(form.keys() - allowed_values)
        raise UnexpectedFormKeys(allowed_keys=allowed_values, unknown_keys=unknown_keys)


class InspectRequestForm:
    dataset: str
    manifest_path: str = None
    manifest_type: str = None
    resource_path: str = None
    resource_type: str = None
    resource_prepare: str = None

    form: FormData = None

    _manifest_tmp_file: tempfile.NamedTemporaryFile = None
    _resource_tmp_file: tempfile.NamedTemporaryFile = None

    def __init__(self, form: FormData):
        _validate_form_data(form)
        self.form = form

    async def init_data(self):
        await self._load_data(
            dataset=self.form.get("dataset", None),
            manifest_type=self.form.get("manifest.type", None),
            manifest_file=self.form.get("manifest.file", None),
            manifest_source=self.form.get("manifest.source", None),
            resource_type=self.form.get("resource.type", None),
            resource_file=self.form.get("resource.file", None),
            resource_source=self.form.get("resource.source", None),
            resource_prepare=self.form.get("resource.prepare", None)
        )

    def clean_up(self):
        if self._resource_tmp_file:
            os.unlink(self._resource_tmp_file.name)
            self._resource_tmp_file = None
        if self._manifest_tmp_file:
            os.unlink(self._manifest_tmp_file.name)
            self._manifest_tmp_file = None

    def get_manifest(self) -> ManifestPath:
        if self.manifest_path:
            _type = self.manifest_type or ManifestPath.type
            return ManifestPath(type=_type, path=self.manifest_path)

    def get_resource(self) -> Tuple:
        if self.resource_path:
            _type = self.resource_type or self.manifest_type or ManifestPath.type
            return _type, self.resource_path

    async def _load_data(
        self,
        dataset: str,
        manifest_type: str = None,
        manifest_file: UploadFile = None,
        manifest_source: str = None,
        resource_file: UploadFile = None,
        resource_source: str = None,
        resource_type: str = None,
        resource_prepare: str = None
    ):
        if manifest_file and manifest_source:
            raise InvalidFormKeyCombination(keys=["manifest.file", "manifest.source"])

        if manifest_source:
            if "http://" not in manifest_source and "https://" not in manifest_source:
                raise InvalidInputData(key="manifest.source", given=manifest_source, condition="it must be URL")
            self.manifest_path = manifest_source
        elif manifest_file:
            tmp = tempfile.NamedTemporaryFile(delete=False)
            read = await manifest_file.read()
            tmp.write(read)
            tmp.close()
            self.manifest_path = tmp.name
            self._manifest_tmp_file = tmp

        self.manifest_type = manifest_type
        if not self.manifest_type and self.manifest_path:
            raise RequiredFormKeyWithCondition(
                key='manifest.type',
                condition="'manifest.source' or 'manifest.file' keys are given"
            )

        if resource_file and resource_source:
            raise InvalidFormKeyCombination(keys=["resource.file", "resource.source"])

        if resource_source:
            if "http://" not in resource_source and "https://" not in resource_source:
                raise InvalidInputData(key="resource.source", given=resource_source, condition="it must be URL")
            self.resource_path = resource_source
        elif resource_file:
            tmp = tempfile.NamedTemporaryFile(delete=False)
            read = await resource_file.read()
            tmp.write(read)
            tmp.close()
            self.resource_path = tmp.name
            self._resource_tmp_file = tmp

        self.dataset = dataset
        self.resource_prepare = resource_prepare

        self.resource_type = resource_type
        if self.resource_path and not self.resource_type:
            raise RequiredFormKeyWithCondition(
                key="resource.type",
                condition="'resource.source' or 'resource.file' keys are given"
            )
        if self.resource_path and not self.dataset:
            raise RequiredFormKeyWithCondition(
                key="dataset",
                condition="'resource.source' or 'resource.file' keys are given"
            )

        if not self.manifest_path and not self.resource_path:
            raise MissingFormKeys(keys=[
                'resource.source',
                'resource.file',
                'manifest.source',
                'manifest.file'
            ])


async def inspect_api(context: Context, request: Request, params: UrlParams):
    check_scope(context, 'inspect')
    if params.format:
        fmt = params.fmt
    else:
        fmt = context.get("config").exporters["csv"]
    form = await request.form()
    inspect_data = InspectRequestForm(form)
    try:
        await inspect_data.init_data()
        context, manifest = create_manifest_from_inspect(
            dataset=inspect_data.dataset,
            manifest=inspect_data.get_manifest(),
            resources=inspect_data.get_resource(),
            formula=inspect_data.resource_prepare,
            only_url=True
        )
        inspect_data.clean_up()
        clean_up_source_for_return(context, manifest)

        return commands.render(
            context,
            manifest,
            fmt,
            action=params.action,
            params=params)
    except Exception as e:
        inspect_data.clean_up()
        raise e


def clean_up_source_for_return(context: Context, manifest: Manifest):
    for dataset in commands.get_datasets(context, manifest).values():
        for resource in dataset.resources.values():
            if resource.external and not ("http://" in resource.external or "https://" in resource.external):
                resource.external = f"https://get.data.gov.lt/{dataset.name}"
