.PHONY: env
env: .venv/pyvenv.cfg var/.done

.venv/pyvenv.cfg: pyproject.toml poetry.toml
	poetry install
	touch --no-create .venv/pyvenv.cfg

var/.done: Makefile
	mkdir -p var/files
	touch var/.done

.PHONY: upgrade
upgrade: .venv/bin/pip-compile
	poetry update

.PHONY: test
test: env
	@# XXX: import-mode=importlib is needed, because `spinta/formats/rdf/components.py`
	@#      and `spinta/manifests/rdf/components.py` have the same basename and
	@#      their parent directories do not have `__init__.py`, so with the
	@#      default prepend import mode pytest fails to collect them with an
	@#      "import file mismatch" error. `SPINTA_ENV=dev` is needed, because
	@#      doctest collection imports `spinta.asgi`, which on import loads the
	@#      default manifest, which requires a `keymaps.default` keymap, defined
	@#      only in the `dev` and `test` environments.
	poetry run py.test -s --full-trace -vvxra --tb=native --log-level=debug --disable-warnings --import-mode=importlib --doctest-modules spinta
	poetry run py.test -vvxra --tb=native --log-level=debug --disable-warnings --cov=spinta --cov-report=term-missing tests

.PHONY: test-github
test-github: 
	poetry run pytest -vvra --tb=short --log-level=debug --cov=spinta --cov-report=term-missing tests

.PHONY: run
run: env
	poetry run uvicorn spinta.asgi:app --reload --log-level debug --host 0.0.0.0

.PHONY: psql
psql:
	PGPASSWORD=admin123 psql -h localhost -p 54321 -U admin -d spinta

format:
	ruff check .
	ruff format .
