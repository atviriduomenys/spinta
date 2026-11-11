.. default-role:: literal

*********
RawConfig
*********

`spinta/core/config.py:RawConfig` is a raw configuration reader. It reads
values directly from several configuration sources, without interpreting
them — no component classes are imported and no typed attributes are built
(that is the job of `Config`, see :doc:`/contrib/architecture/configuration/index`). Every value keeps a
reference to the source it came from, so origins can be reported with
`spinta config`.

Sources
*******

`RawConfig` holds a list of sources — instances of
`spinta/core/config.py:ConfigSource` subclasses:

- `ConfigSource` — base class. Each source has a `name` (used as the value
  origin) and a flat `config` dict, where keys are tuples, e.g.
  `('backends', 'default', 'dsn')`. `ConfigSource.read(schema)` is
  responsible for flattening the raw data into that shape.
- `PyDict` — a Python dict with dotted keys, e.g.
  `{'backends.default': {'dsn': ...}}`. Dotted keys are split into tuples
  and a top-level `environments` sub-dict is moved under
  `('environments', <env>)` prefixes.
- `Path(PyDict)` — a YAML file path (`.yml`/`.yaml`) or a Python dotted path
  to a dict (e.g. `spinta.config:CONFIG`, the built-in defaults).
- `CliArgs(PyDict)` — `-o key=value` arguments (name `cli`). Values
  containing commas are split into lists, e.g.
  `-o backends=pg,fs`.
- `EnvVars` — `SPINTA_` prefixed environment variables (name `env`). The
  prefix is stripped, the name is lowercased and `__` separators become
  nesting levels: `SPINTA_BACKENDS__DEFAULT__DSN` is stored as
  `('backends', 'default', 'dsn')`. Names longer than one level, where the
  first segment is not a known config key but the second one is, are treated
  as environment qualified keys: `SPINTA_MYENV__SERVER_URL` is stored as
  `('environments', 'myenv', 'server_url')`.
- `EnvFile(EnvVars)` — a `.env` file (name `envfile`); `KEY=value` lines are
  parsed and then processed exactly like environment variables.

Reading and looking up values
*****************************

- `rc.read(sources, after=None)` — reads and appends sources. If `after` is
  given, sources are inserted after the source with that name (used for
  extension provided defaults, inserted after the built-in `spinta`
  defaults).
- `rc.get(*key, default=NA, cast=None, required=False, exists=False,
  origin=False)` — looks a value up from the last source backwards, so later
  sources override earlier ones. When no source defines the value, the
  default from the `spinta/config.yml` schema (`SCHEMA`) is used. `cast`
  converts the value (`importstr`, `int`, `bool`, ...), `origin=True` makes
  `get` return a `(value, source_name)` tuple. The sentinel for "no value"
  is `spinta.utils.schema.NA`.
- `rc.get(key, env)` — internally, when the `env` option is set (e.g.
  `SPINTA_ENV=dev`), each source is first queried with the
  `('environments', env)` prefix, so environment specific values override
  plain values of the same source.
- `rc.keys(*key)` — sub-keys of a config object. `RawConfig` keeps an inner
  key index (`_update_keys`), so nested objects can be traversed even when
  they were only "announced" by a comma separated parent value, e.g.
  `SPINTA_BACKENDS=pg,fs` followed by `SPINTA_BACKENDS__PG__DSN=...`.
- `rc.getall(*key, origin=False)` — iterates all leaf values under a key.
- `rc.dump(*names, fmt=KeyFormat.cfg, file=sys.stdout)` — prints the
  origin / name / value table used by the `spinta config` command;
  `KeyFormat.env` renders names as `SPINTA_` environment variables.
- `rc.has(*key)` — checks whether a key is set.
- `rc.to_dict(*names)` — converts a config section back into a nested dict.

The schema
**********

`RawConfig._schema` is `spinta/core/config.py:SCHEMA` — a dict built from
`spinta/config.yml`. It declares the type of every option, nested objects,
dynamic key groups (`keys: {type: string}`) and type specific variants
(`switch: type` with `case:` blocks). `RawConfig` walks this schema when
resolving defaults and when classifying keys; it does not reject unknown
keys, they simply have no defaults.

Fork and lock
*************

- `rc.fork(sources=None, after=None)` — returns a new `RawConfig` with the
  same source list; given sources are appended (a plain dict is added as a
  source named `fork`). Used by `spinta/core/config.py:configure_rc()` to
  adjust configuration per command (default backends, keymap and manifest
  entries), without mutating the original raw config.
- `rc.add(name, params)` — convenience for reading a single dict source.
- `rc.lock()` — forbids further `read()` calls ("Configuration is locked,
  use `rc.fork()` if you need to change configuration."). The test session
  fixture locks the raw config in `spinta/testing/pytest.py`.

External specifications
***********************

`RawConfig` is Spinta's own mechanism — UAPI and DSA do not specify how a
server is configured. Related external concepts:

- The Twelve-Factor App, "Config" (https://12factor.net/config) — the
  layered sources with environment variables overriding files follow the
  twelve-factor configuration principle.
- XDG Base Directory Specification
  (https://specifications.freedesktop.org/basedir-spec/latest/) — the
  default `config_path` / `data_path` fallbacks (`DEFAULT_CONFIG_PATH`,
  `DEFAULT_DATA_PATH` in `spinta/core/config.py`).

See also
********

- :doc:`/contrib/architecture/configuration/index` — the runtime `Config` component.
- :doc:`/contrib/architecture/configuration/index` — the configuration system overview.
- :doc:`/reference/components` — autodoc API of `RawConfig`.
