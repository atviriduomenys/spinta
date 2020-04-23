import cgi
import tempfile
import typing

import boto3
import botocore

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta import commands
from spinta.backends import Backend, simple_data_check
from spinta.commands.write import prepare_patch, simple_response, validate_data
from spinta.components import Action, Context, DataItem, Model, Property, UrlParams
from spinta.core.config import RawConfig
from spinta.exceptions import ItemDoesNotExist
from spinta.manifests.components import Manifest
from spinta.renderer import render
from spinta.types.datatype import File
from spinta.utils.aiotools import aiter


class S3(Backend):
    pass


@commands.load.register()
def load(context: Context, backend: S3, config: RawConfig):
    backend.bucket_name = config.get('backends', backend.name, 'bucket', required=True)
    backend.region = config.get('backends', backend.name, 'region', required=True)

    access_key = config.get('backends', backend.name, 'access_key_id')
    secret_key = config.get('backends', backend.name, 'secret_access_key')
    backend.credentials = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
    }


@commands.wait.register()
def wait(context: Context, backend: S3, *, fail: bool = False):
    try:
        # create s3 client and check if we can list our buckets (a way to
        # make requests to s3 to make sure that aws credentials are ok)
        client = get_aws_session(backend).client('s3')
        client.list_buckets()
        return True
    except botocore.exceptions.ClientError:
        return False


@commands.prepare.register()
def prepare(context: Context, backend: S3, manifest: Manifest):
    client = get_aws_session(backend).client('s3')
    try:
        client.get_bucket_location(Bucket=backend.bucket_name)
    except client.exceptions.NoSuchBucket:
        client.create_bucket(
            ACL='private',
            Bucket=backend.bucket_name,
            CreateBucketConfiguration={'LocationConstraint': backend.region},
        )


@commands.bootstrap.register()
def bootstrap(context: Context, backend: S3):
    pass


@commands.migrate.register()
def migrate(context: Context, backend: S3):
    pass


@commands.push.register()
async def push(
    context: Context,
    request: Request,
    dtype: File,
    backend: S3,
    *,
    action: Action,
    params: UrlParams,
):
    prop = dtype.prop
    commands.authorize(context, action, prop)
    data = DataItem(
        prop.model,
        prop,
        propref=False,
        backend=backend,
        action=action
    )
    data.given = {
        prop.name: {
            '_content_type': request.headers.get('content-type'),
            '_id': None,
        }
    }

    if 'revision' in request.headers:
        data.given['_revision'] = request.headers.get('revision')
    if 'content-disposition' in request.headers:
        data.given[prop.name]['_id'] = cgi.parse_header(request.headers['content-disposition'])[1]['filename']

    if not data.given[prop.name]['_id']:
        # XXX: Probably here should be a new UUID.
        data.given[prop.name]['_id'] = params.pk

    simple_data_check(context, data, data.prop, data.model.backend)

    data.saved = getone(context, prop, dtype, prop.model.backend, id_=params.pk)

    filename = data.given[prop.name]['_id']
    if action == Action.UPDATE:
        if 'content-length' not in request.headers:
            raise HTTPException(status_code=411)
        dstream = aiter([data])
        dstream = validate_data(context, dstream)
        dstream = prepare_patch(context, dstream)
        dstream = upload_file_to_s3(backend, filename, dstream, request.stream())
        dstream = commands.update(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )
    elif action == Action.DELETE:
        dstream = aiter([data])
        dstream = validate_data(context, dstream)
        dstream = prepare_patch(context, dstream)
        dstream = commands.delete(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )
    else:
        raise Exception(f"Unknown action {action!r}.")

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, status_code=status_code)


async def upload_file_to_s3(
    backend: S3,
    filename: str,
    dstream: typing.AsyncIterator[DataItem],
    fstream: typing.AsyncIterator[bytes]
) -> typing.AsyncGenerator[DataItem, None]:
    aws_session = get_aws_session(backend)
    s3_bucket = aws_session.resource('s3').Bucket(backend.bucket_name)

    async for d in dstream:
        with tempfile.TemporaryFile() as f:
            async for chunk in fstream:
                f.write(chunk)
            f.seek(0)
            s3_bucket.upload_fileobj(f, filename)
        yield d


@commands.getone.register()
async def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: File,
    backend: S3,
    *,
    action: str,
    params: UrlParams,
):
    commands.authorize(context, action, prop)
    data = getone(context, prop, prop.dtype, prop.model.backend, id_=params.pk)
    value = data[prop.name]
    filename = value['_id']
    if filename is None:
        raise ItemDoesNotExist(prop, id=params.pk)

    client = get_aws_session(backend).client('s3')
    obj = client.get_object(Bucket=backend.bucket_name, Key=filename)
    data_gen = obj['Body'].iter_chunks()

    return StreamingResponse(
        aiter(data_gen),
        media_type=value.get('_content_type'),
        headers={
            'Revision': data['_revision'],
            'Content-Disposition': f'attachment; filename="{filename}"',
        },
    )


@commands.wipe.register()
def wipe(context: Context, model: Model, backend: S3):
    s3_client = get_aws_session(backend).client('s3')
    objs = s3_client.list_objects(Bucket=bucket_name)
    obj_keys = {'Objects': [
        {'Key': obj['Key'] for obj in objs['Contents']}
    ]}
    bucket = s3.Bucket(bucket_name)
    bucket.delete_objects(Delete=obj_keys)


def get_aws_session(backend: Backend, **kwargs):
    return boto3.Session(region_name=backend.region,
                         **backend.credentials,
                         **kwargs)
