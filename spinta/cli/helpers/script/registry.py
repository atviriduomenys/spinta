from __future__ import annotations

import importlib
import pkgutil

from mypy.checkexpr import defaultdict

from spinta.cli.helpers.script.components import ScriptBase
from spinta.exceptions import ScriptNotFound


class _ScriptRegistryView:
    registry: _ScriptRegistry
    script_type: str

    def __init__(self, registry: _ScriptRegistry, script_type: str):
        self.registry = registry
        self.script_type = script_type

    def get(self, script_name: str) -> ScriptBase:
        return self.registry.get(self.script_type, script_name)

    def get_all(self, **kwargs) -> dict[str, ScriptBase]:
        return self.registry.get_all(self.script_type, **kwargs)

    def get_all_names(self, **kwargs) -> tuple[str]:
        return self.registry.get_all_names(self.script_type, **kwargs)

    def contains(self, script_name: str) -> bool:
        return self.registry.contains(self.script_type, script_name)


class _ScriptRegistry:
    _registry: dict[str, dict[str, ScriptBase]]

    def __init__(self):
        self._registry = defaultdict(dict)

    def register(self, script: ScriptBase):
        scripts = self._registry[script.script_type]
        if script.name in scripts:
            raise ValueError(f"Duplicate script name: {script.name}")
        scripts[script.name] = script

    def get(self, script_type: str, script_name: str) -> ScriptBase:
        scripts = self._registry[script_type]
        if script_name not in scripts:
            raise ScriptNotFound(
                script_type=script_type, script=script_name, available_scripts=self.get_all_names(script_type)
            )
        return scripts[script_name]

    def get_all(self, script_type: str, targets: set | None = None, tags: set | None = None) -> dict[str, ScriptBase]:
        scripts = self._registry[script_type]
        if not targets and not tags:
            return scripts

        scripts = self._registry[script_type]
        filtered_scripts = {}
        for script_name, script in scripts.items():
            if targets and not targets.intersection(script.targets):
                continue
            if tags and not tags.intersection(script.tags):
                continue

            filtered_scripts[script_name] = script

        return filtered_scripts

    def get_all_names(self, script_type: str, targets: set | None = None, tags: set | None = None) -> tuple[str]:
        scripts = self.get_all(script_type, targets, tags)
        return tuple(scripts.keys())

    def contains(self, script_type: str, script_name: str) -> bool:
        return script_name in self._registry[script_type]

    def view(self, script_type: str) -> _ScriptRegistryView:
        return _ScriptRegistryView(self, script_type)


script_registry = _ScriptRegistry()


# You can set what module to import first (in case scripts are defined in multiple modules),
# Or you can call script_registry.register(script) manually
def import_all_modules_from(package_name: str):
    package = importlib.import_module(package_name)
    for _, modname, _ in pkgutil.walk_packages(package.__path__, prefix=package.__name__ + "."):
        importlib.import_module(modname)


import_all_modules_from("spinta.cli.helpers.upgrade")
import_all_modules_from("spinta.cli.helpers.admin")
