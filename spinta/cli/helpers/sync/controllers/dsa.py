from http import HTTPStatus

import requests

from spinta.cli.helpers.sync import CONTENT_TYPE_TEXT_CSV
from spinta.cli.helpers.sync.helpers import check_api_response
from spinta.exceptions import NotImplementedFeature


def create_dsa(base_path, headers, dataset_id, content):
    response = requests.post(
        f"{base_path}/Dataset/{dataset_id}/dsa/",
        headers={"Content-Type": CONTENT_TYPE_TEXT_CSV, **headers},
        data=content,
    )
    check_api_response(response, {HTTPStatus.NO_CONTENT}, "Create DSA")


def update_dsa(base_path, headers, dataset_id):
    # TODO: Unfinished: implement w/ https://github.com/atviriduomenys/katalogas/issues/1600.
    response = requests.put(f"{base_path}/Dataset/{dataset_id}/dsa/", headers=headers)
    raise NotImplementedFeature(
        status=response.status_code,
        dataset_id=dataset_id,
        feature="Updates on existing Datasets",
    )
