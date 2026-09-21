# Spinta architecture

Not written yet. This section describes the overall structure of Spinta that
feature specifications (`../specifications/`) build on:

- loading manifests and linking nodes (`Model`, `Property`, `Dataset`,
  `Namespace`);
- commands (`spinta.commands`) and `multipledispatch` dispatch;
- storage backends (`backends`) and external data sources (`datasets`);
- the HTTP API: query parameters, formats, authorization;
- the CLI (`spinta.cli`).

One file per topic in this directory.
