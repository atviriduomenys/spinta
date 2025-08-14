from http import HTTPStatus

import requests

from spinta.cli.helpers.sync.helpers import check_api_response


def get_dataset(base_path, headers, dataset_name):
    response = requests.get(
        f"{base_path}/Dataset/",
        headers=headers,
        params={"name": dataset_name},
    )
    check_api_response(response, {HTTPStatus.OK, HTTPStatus.NOT_FOUND}, "Get dataset")
    return response


def create_dataset(base_path, headers, dataset_name):
    response = requests.post(
        f"{base_path}/Dataset/",
        headers=headers,
        data={
            "title": dataset_name,
            "description": "",
            "name": dataset_name,
        }
    )
    check_api_response(response, {HTTPStatus.CREATED}, "Create dataset")
    return response
