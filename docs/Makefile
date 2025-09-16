# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
VENVDIR       = ../.venv
SPHINXOPTS    ?=
SPHINXBUILD   ?= $(VENVDIR)/bin/sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

.venv/bin/pip:
	python -m venv .venv
	.venv/bin/pip install -r requirements.txt

.venv/bin/pip-compile: .venv/bin/pip
	.venv/bin/pip install pip-tools

requirements.txt: .venv/bin/pip-compile requirements.in
	.venv/bin/pip-compile requirements.in -o requirements.txt

.PHONY: upgrade
upgrade: .venv/bin/pip-compile
	.venv/bin/pip-compile --upgrade requirements.in -o requirements.txt

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

auto:
		$(VENVDIR)/bin/sphinx-autobuild --port 8000 -b html $(SOURCEDIR) $(BUILDDIR)/html

open:
		xdg-open http://127.0.0.1:8000

.PHONY: help auto open Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
