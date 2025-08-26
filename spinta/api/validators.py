from typing import Optional, Any

from pydantic import BaseModel, Field, ConfigDict


class ClientAddData(BaseModel):
    client_name: Optional[str] = None
    secret: str = Field(..., min_length=1)
    scopes: Optional[list[str]] = None
    backends: Optional[dict[str, dict[str, Any]]] = None

    model_config = ConfigDict(extra="forbid")


class ClientSecretPatchData(BaseModel):
    secret: Optional[str] = Field(default=None, min_length=1)

    model_config = ConfigDict(extra="forbid")


class ClientPatchData(ClientSecretPatchData):
    client_name: Optional[str] = None
    scopes: Optional[list[str]] = None
    backends: Optional[dict[str, dict[str, Any]]] = None
