import logging

import requests
from typer import Context as TyperContext, echo

from spinta.auth import DEFAULT_CREDENTIALS_SECTION
from spinta.cli.helpers.store import prepare_manifest
from spinta.client import get_client_credentials, RemoteClientCredentials, get_access_token
from spinta.components import Config
from spinta.core.context import configure_context

logger = logging.getLogger(__name__)


def sync(ctx: TyperContext):
    # TODO update sync logic https://github.com/atviriduomenys/spinta/issues/1310
    context = configure_context(ctx.obj, mode="external")
    prepare_manifest(context, full_load=True)
    config: Config = context.get('config')
    credentials :RemoteClientCredentials= get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)
    access_token = get_access_token(credentials)
    resource_server = credentials.resource_server or credentials.server
    response = requests.post(
        f'{resource_server}/organizations/agents/sync',
        headers={'Authorization': f'Bearer {access_token}'}
    )
    logger.info(response.text)
    echo(response.text)
