.PHONY: env
env: .env env/.done var/.done requirements.txt

env/bin/pip:
	python3.7 -m venv env
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

.env: .env.example
	cp -n .env.example .env | true
	touch .env

.PHONY: upgrade
upgrade: env/bin/pip-compile
	env/bin/pip-compile --upgrade --no-index requirements.in requirements-dev.in -o requirements-dev.txt
	env/bin/pip-compile --upgrade --no-index requirements.in -o requirements.txt

.PHONY: test
test: env
	env/bin/py.test -vvxra --log-level=debug --cov=spinta --cov-report=term-missing --doctest-modules tests spinta

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
