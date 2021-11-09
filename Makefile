.PHONY: env
env: .env .venv/pyvenv.cfg var/.done requirements.txt docs/requirements.txt

.venv/pyvenv.cfg: pyproject.toml poetry.toml
	poetry install
	touch --no-create .venv/pyvenv.cfg

var/.done: Makefile
	mkdir -p var/files
	touch var/.done

docs/requirements.txt: env/bin/pip-compile docs/requirements.in
	env/bin/pip-compile --no-emit-index-url docs/requirements.in -o docs/requirements.txt

requirements.txt: poetry.lock
	poetry export -f requirements.txt -o requirements.txt

.env: .env.example
	cp -n .env.example .env | true
	touch .env

.PHONY: upgrade
upgrade: env/bin/pip-compile
	poetry update
	poetry export -o requirements.txt

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

.PHONY: dist
dist: env/bin/pip
	poetry build

.PHONY: publish
publish:
	poetry publish

.PHONY: run
run: env
	AUTHLIB_INSECURE_TRANSPORT=1 poetry run uvicorn spinta.asgi:app --debug

.PHONY: psql
psql:
	PGPASSWORD=admin123 psql -h localhost -p 54321 -U admin -d spinta


.PHONY: docs-auto
docs-auto:
	$(MAKE) -C docs auto

.PHONY: docs-open
docs-open:
	$(MAKE) -C docs open
