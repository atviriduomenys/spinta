import pathlib

from requests.models import Response


def parse_csv(resp: Response) -> list[list[str]]:
    resp.raise_for_status()
    return [line.strip().split(",") for line in resp.text.splitlines()]


def read_csv(path: pathlib.Path) -> list[list[str]]:
    with path.open("r") as f:
        return [line.strip().split(",") for line in f.readlines()]
