from importlib import import_module

import_module("spinta.manifests.wsdl.commands")

from .helpers import is_wsdl_path, normalize_wsdl_path

__all__ = ["is_wsdl_path", "normalize_wsdl_path"]