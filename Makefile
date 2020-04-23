.PHONY: env
env: .env env/.done var/.done requirements.txt docs/requirements.txt

env/bin/pip:
	python3.8 -m venv env
	env/bin/pip install --upgrade pip wheel setuptools

env/.done: env/bin/pip setup.py requirements-dev.txt
	env/bin/pip install -r requirements-dev.txt -e .
	touch env/.done

var/.done: Makefile
	mkdir -p var/files
	touch var/.done

env/bin/pip-compile: env/bin/pip
	env/bin/pip install pip-tools

requirements-dev.txt: env/bin/pip-compile requirements.in requirements-dev.in
	env/bin/pip-compile --no-index requirements.in requirements-dev.in -o requirements-dev.txt

requirements.txt: env/bin/pip-compile requirements.in
	env/bin/pip-compile --no-index requirements.in -o requirements.txt

docs/requirements.txt: env/bin/pip-compile docs/requirements.in
	env/bin/pip-compile --no-index docs/requirements.in -o docs/requirements.txt

.env: .env.example
	cp -n .env.example .env | true
	touch .env

.PHONY: upgrade
upgrade: env/bin/pip-compile
	env/bin/pip-compile --upgrade --no-index requirements.in requirements-dev.in -o requirements-dev.txt
	env/bin/pip-compile --upgrade --no-index requirements.in -o requirements.txt

.PHONY: test
test: env
	@# XXX: I have no idea woath is going on, but if I run doctests together
	@#      with other tests in `py.test --doctest-modules tests spinta`, then
	@#      for some reason `spinta.config:CONFIG` looses 'environments' item.
	@#      Could not found reason why this happens, bet if I remove `spinta`
	@#      from test paths, then tests pass. Maybe this has something to do
	@#      with py.test?
	env/bin/py.test -s --full-trace -vvxra --tb=native --log-level=debug --disable-warnings --doctest-modules spinta
	env/bin/py.test -vvxra --tb=native --log-level=debug --disable-warnings --cov=spinta --cov-report=term-missing tests

.PHONY: dist
dist: env/bin/pip
	env/bin/python setup.py sdist bdist_wheel

.PHONY: publish
publish:
	twine upload dist/*

.PHONY: run
run: env
	AUTHLIB_INSECURE_TRANSPORT=1 env/bin/uvicorn spinta.asgi:app --debug

.PHONY: psql
psql:
	PGPASSWORD=admin123 psql -h localhost -p 54321 -U admin -d spinta


.PHONY: docs-auto
docs-auto:
	$(MAKE) -C docs auto

.PHONY: docs-open
docs-open:
	$(MAKE) -C docs open
