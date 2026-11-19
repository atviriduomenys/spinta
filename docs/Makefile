# Makefile for Sphinx documentation.
#
# Documentation sources are split by language: `en` and `lt`, each has its
# own `conf.py`. HTML output is written to `_build/html/<language>/`.
#
# Sphinx and documentation dependencies are installed into a local `.venv`
# from `requirements.txt` (which is compiled from `requirements.in` with
# pip-tools), because these dependencies are not needed to run Spinta itself
# and are not managed by Poetry.

VENVDIR       = .venv
SPHINXOPTS    ?=
SPHINXBUILD   ?= $(VENVDIR)/bin/sphinx-build
SOURCEDIR     = en
BUILDDIR      = _build
PORT          ?= 8101

$(VENVDIR)/bin/sphinx-build:
	python3 -m venv $(VENVDIR)
	$(VENVDIR)/bin/pip install -r requirements.txt

$(VENVDIR)/bin/pip-compile: $(VENVDIR)/bin/pip
	$(VENVDIR)/bin/pip install pip-tools

.PHONY: requirements
requirements: $(VENVDIR)/bin/pip-compile
	$(VENVDIR)/bin/pip-compile requirements.in -o requirements.txt
	$(VENVDIR)/bin/pip install -r requirements.txt

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  html   build HTML documentation for all languages ($(LANGUAGES))"
	@echo "  auto   start sphinx-autobuild for the English documentation"
	@echo "  clean  remove build output"

.PHONY: auto
auto: $(VENVDIR)/bin/sphinx-build
	$(VENVDIR)/bin/sphinx-autobuild --port $(PORT) -b html $(SOURCEDIR) $(BUILDDIR)/html/en $(SPHINXOPTS)

.PHONY: open
open:
	xdg-open http://127.0.0.1:$(PORT)

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)
