.. default-role:: literal

******
Config
******

`spinta/components.py:Config` is the runtime configuration component — the
place where all configuration options are stored and used while Spinta is
running. It is a typed counterpart of the raw, layered configuration kept in
`RawConfig` (see :doc:`/contrib/architecture/configuration/rawconfig`).

Creation and lifecycle
**********************

- The `Config` class used at runtime is itself configurable: it is looked up
  in the `components.core.config` registry (default
  `spinta.components:Config`) and instantiated in
  `spinta/core/context.py:create_context()`, together with `Context` and
  `Store`.
- Right after instantiation the object is almost empty — just a back
  reference to the raw config and empty registries.
- The values are filled in by the `load` command, implemented as
  `load(Context, Config)` in `spinta/types/config.py`. This is where
  `RawConfig` values are parsed, cast to Python types and assigned to
  `Config` attributes.
- `check(Context, Config)` (also in `spinta/types/config.py`) validates the
  loaded configuration, e.g. that the `default_auth_client` exists in the
  clients directory under `config_path`.

The raw config stays reachable at `config.rc`, so components can read raw
keys directly when needed (this is how `Store` loads backends, keymaps and
manifests from `rc`, see :doc:`/contrib/architecture/configuration/index`).

Registries
**********

`Config` holds several registries populated from the `components` and
related config sections (all values are `importstr` entries in
`spinta/config.py:CONFIG`):

- `config.components` — dict of component groups, e.g.
  `components.core`, `components.manifests`, `components.backends`,
  `components.keymaps`, `components.types`, `components.querybuilders`,
  `components.resultbuilders`, `components.accesslog`, ... Each group maps
  names to classes, e.g. `config.components['backends']['postgresql']` is
  `spinta.backends.postgresql.components:PostgreSQL`.
- `config.commands` — named service functions from the `commands.service`
  config section (e.g. `commands.service.range` →
  `spinta.commands.helpers:range_`), see :doc:`/contrib/architecture/multiple-dispatch/commands`.
- `config.exporters` — exporter format names mapped to instantiated
  `Format` objects (`spinta/formats/`).
- `config.AccessLog` — the access log class selected by
  `accesslog.type` (`FileAccessLog` or `PythonAccessLog`).
- `config.resolvers` and `config.executors` — ufunc registries collected
  from `CONFIG['ufuncs']` modules, used by `Env` (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index`
  and :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc`).

Options
*******

The rest of the attributes are plain configuration options. The main groups:

- paths — `config_path` (defaults to `~/.config/spinta`),
  `data_path` (`~/.local/share/spinta`), `credentials_file`,
  `docs_path`, `downloaded_public_keys_file`;
- auth — `scope_prefix` (`spinta_`), `scope_prefix_udts` (`uapi:/`),
  `scope_formatter`, `scope_max_length`, `default_auth_client`,
  `default_access_level`, `access`, `token_validation_key`,
  `token_issuer`, `resource_server`;
- API behavior — `server_url`, `root`, `default_page_size`,
  `enable_pagination`, `sync_page_size`, `languages`,
  `cache_control`, `http_strict_transport_security`,
  `health_min_free_disk_space`, `health_min_free_memory`;
- data sync — `sync_retry_count`, `sync_retry_delay_range`,
  `max_file_size`, `max_error_count_on_insert`;
- operation — `debug`, `env`, `always_show_id`, `check_names`,
  `ensure_backends`, `upgrade_mode`, `log_level`, `file_log_level`,
  `file_log_path`, `default_distribution_strategy` (a
  `DistributionStrategy` value built from `default_distribution_strategy`
  and `default_distribution_property`).

A few attributes are managed as containers alongside the options:
`config.backends`, `config.manifests` and `config.ignore` are initialized
as empty containers (actual backends, keymaps and manifests are instantiated
on the `Store` component, and the `ignore` list is read per manifest by
`spinta/manifests/yaml/helpers.py`), `config.datasets` is read from the
`datasets` config section, and `config.front_page_warning` is read from
`texts.front_page_warning`.

External specifications
***********************

Configuration is not part of the UAPI or DSA specifications, but several
options implement concepts from them:

- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`) — OAuth 2.0. `token_issuer` and
  `resource_server` correspond to the authorization server and resource
  server roles (RFC 6749 section 1.1); `default_auth_client` and the files
  under `config_path/clients` are OAuth client registrations.
- The Twelve-Factor App, "Config" (https://12factor.net/config) — the
  `SPINTA_*` environment variable layer and the `env` option
  (`dev`/`test`/...) follow the environment-based configuration pattern.
- XDG Base Directory Specification
  (https://specifications.freedesktop.org/basedir-spec/latest/) — default
  `config_path` and `data_path` locations.

See also
********

- :doc:`/contrib/architecture/configuration/rawconfig` — the raw, layered configuration reader.
- :doc:`/contrib/architecture/configuration/index` — the configuration system overview.
- :doc:`/reference/components` — autodoc API of `Config`.
