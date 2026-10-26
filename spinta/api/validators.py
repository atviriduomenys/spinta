from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ClientAddData(BaseModel):
    client_name: str | None = None
    secret: str = Field(..., min_length=1)
    scopes: list[str] | None = None

    model_config = ConfigDict(extra="forbid")


class ClientSecretPatchData(BaseModel):
    secret: str | None = Field(default=None, min_length=1)

    model_config = ConfigDict(extra="forbid")


class ClientBackendsData(ClientSecretPatchData):
    backends: dict[str, dict[str, Any]] | None = None


class ClientPatchData(ClientBackendsData):
    client_name: str | None = None
    scopes: list[str] | None = None
