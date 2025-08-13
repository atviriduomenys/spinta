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
	@# XXX: I have no idea what is going on, but if I run doctests together
	@#      with other tests in `py.test --doctest-modules tests spinta`, then
	@#      for some reason `spinta.config:CONFIG` looses 'environments' item.
	@#      Could not found reason why this happens, bet if I remove `spinta`
	@#      from test paths, then tests pass. Maybe this has something to do
	@#      with py.test?
	poetry run py.test -s --full-trace -vvxra --tb=native --log-level=debug --disable-warnings --doctest-modules spinta
	poetry run py.test -vvxra --tb=native --log-level=debug --disable-warnings --cov=spinta --cov-report=term-missing tests

.PHONY: test-github
test-github: 
	poetry run pytest -vvxra --tb=short --log-level=debug --cov=spinta --cov-report=term-missing tests

.PHONY: run
run: env
	poetry run uvicorn spinta.asgi:app --reload --log-level debug --host 0.0.0.0

.PHONY: psql
psql:
	PGPASSWORD=admin123 psql -h localhost -p 54321 -U admin -d spinta
