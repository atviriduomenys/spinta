import cgi
import tempfile
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Dict, Any

import boto3
import botocore

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta import commands
from spinta.backends import Backend, simple_data_check, log_getone
from spinta.backends.components import BackendFeatures
from spinta.commands import getall
from spinta.commands.write import prepare_patch, simple_response, validate_data, log_write
from spinta.components import Action, Context, DataItem, Property, UrlParams
from spinta.exceptions import ItemDoesNotExist
from spinta.manifests.components import Manifest
from spinta.renderer import render
from spinta.types.datatype import File
from spinta.utils.aiotools import aiter
from spinta.utils.data import take


class S3(Backend):

    bucket_name: str = None
    region: str = None

    features = {
        BackendFeatures.WRITE,
    }


@commands.load.register(Context, S3, dict)
def load(context: Context, backend: S3, config: Dict[str, Any]):
    backend.bucket_name = config['bucket']
    backend.region = config['region']
    backend.credentials = {
        'aws_access_key_id': config.get('access_key_id'),
        'aws_secret_access_key': config.get('secret_access_key'),
    }


@commands.wait.register(Context, S3)
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


@commands.migrate.register(Context, S3)
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

    dstream = aiter([data])
    dstream = validate_data(context, dstream)
    dstream = prepare_patch(context, dstream)
    dstream = log_write(context, dstream)
    filename = data.given[prop.name]['_id']
    if action == Action.UPDATE:
        if 'content-length' not in request.headers:
            raise HTTPException(status_code=411)
        dstream = upload_file_to_s3(backend, filename, dstream, request.stream())
        dstream = commands.update(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )
    elif action == Action.DELETE:
        dstream = commands.delete(context, prop, dtype, prop.model.backend, dstream=dstream)
        dstream = commands.create_changelog_entry(
            context, prop.model, prop.model.backend, dstream=dstream,
        )
    else:
        raise Exception(f"Unknown action {action!r}.")

    status_code, response = await simple_response(context, dstream)
    return render(context, request, prop, params, response, action=action, status_code=status_code)


async def upload_file_to_s3(
    backend: S3,
    filename: str,
    dstream: AsyncIterator[DataItem],
    fstream: AsyncIterator[bytes]
) -> AsyncGenerator[DataItem, None]:
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
    log_getone(context, data)
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


@commands.wipe.register(Context, File, S3)
def wipe(context: Context, dtype: File, backend: S3):
    aws = get_aws_session(backend)
    objs = aws.client('s3').list_objects(Bucket=backend.bucket_name)

    if 'Contents' in objs:
        obj_keys = {'Objects': []}
        fnames = [obj['Key'] for obj in objs['Contents']]
        rows = getall(context, dtype.prop.model, dtype.prop.model.backend)
        for r in rows:
            fn = take(dtype.prop.name + '._id', r)
            if fn and fn in fnames:
                obj_keys['Objects'].append({'Key': fn})
        if obj_keys['Objects']:
            bucket = aws.resource('s3').Bucket(backend.bucket_name)
            bucket.delete_objects(Delete=obj_keys)


def get_aws_session(backend: Backend, **kwargs):
    return boto3.Session(region_name=backend.region,
                         **backend.credentials,
                         **kwargs)
