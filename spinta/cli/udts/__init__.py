"""CLI commands of the UDTS agent.

Everything under `spinta udts` is specific to Spinta acting as an UDTS data
service agent and is not needed for open data publishing.
"""

from typer import Typer

from spinta.cli.udts.oas import oas

app = Typer()

app.command("oas", short_help="Export OpenAPI specification of an UDTS data service")(oas)
