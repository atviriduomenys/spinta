from __future__ import annotations

from pathlib import Path
from uuid import UUID

from typer import Argument, Typer
from typer import Context as TyperContext
from typer import Option
from rich import print

from spinta.auth import client_exists, create_client_file, update_client_file
from spinta.cli.helpers.contracts.helpers import extract_elements_from_adoc, SCOPES_REGEX
from spinta.cli.helpers.sync.helpers import prepare_context
from spinta.exceptions import InvalidAdocError
from spinta.utils.config import get_clients_path
from spinta.utils.types import is_str_uuid

contract_command = Typer()


def validate_contract_filename(contract_filename: str) -> None:
    """Contract file name must be UUID"""
    if not is_str_uuid(contract_filename.replace(".adoc", "")):
        raise InvalidAdocError(f"Invalid ADOC filename: ADOC filename '{contract_filename}' must be UUID")


def get_contract_file_path(contract_filename: str) -> Path:
    if not contract_filename.endswith(".adoc"):
        contract_filename = f"{contract_filename}.adoc"

    validate_contract_filename(contract_filename)

    return Path(f"{contract_filename[0:2]}/{contract_filename[2:4]}/{contract_filename[4:]}")


@contract_command.command("parse", short_help="Parse smart contract scopes from .adoc file and save it to client files")
def contract_parse(
    ctx: TyperContext,
    contract_files: list[str] = Argument(..., help="Smart contract file names to parse"),
    client_ids: list[UUID] | None = Option(None, "--clients", help="Client ids to which scopes should be saved"),
) -> None:
    context = prepare_context(ctx.obj)
    contract_dir_path = context.get("config").contract_dir_path

    parsed_scopes = {}
    for contract_name in contract_files:
        contract_file_path = get_contract_file_path(contract_name)
        contract_path = contract_dir_path / contract_file_path

        if not contract_path.exists():
            raise InvalidAdocError(f"Invalid ADOC: ADOC {contract_path} does not exist")

        parsed_scopes[str(contract_file_path)] = extract_elements_from_adoc(str(contract_path), regex=SCOPES_REGEX)

    if not client_ids:
        print("No client ids specified. Printing parsed scopes to stdout:")
        print(parsed_scopes)
        return

    clients_path = get_clients_path(context.get("config"))
    for client_id in client_ids:
        if client_exists(clients_path, str(client_id)):
            update_client_file(
                context=context,
                path=clients_path,
                client_id=str(client_id),
                name=None,
                secret=None,
                scopes=None,
                backends=None,
                contract_scopes=parsed_scopes,
            )
        else:
            create_client_file(
                path=clients_path,
                name=str(client_id),
                client_id=str(client_id),
                contract_scopes=parsed_scopes,
            )

    print(f"Parsed scopes saved to clients {', '.join(str(client_id) for client_id in client_ids)}")
