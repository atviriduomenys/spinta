# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= ../env/bin/sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

export PATH := ../env/bin:$(PATH)

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

auto:
		../env/bin/sphinx-autobuild -p 8080 -b html $(SOURCEDIR) $(BUILDDIR)/html

open:
		xdg-open http://127.0.0.1:8000

.PHONY: help auto open Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
