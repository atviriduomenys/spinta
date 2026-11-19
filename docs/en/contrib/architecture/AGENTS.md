# AGENTS.md — architecture documentation

Guidance for agents (and humans) writing or editing the pages in this
directory. The entry point is [index.rst](index.rst); the reference example of
the pattern described here is [sync.rst](sync.rst) and its `sync/` subpages.

## Main principles

1. **Describe the target architecture, then the current state.** A page
   describes how a part of Spinta is meant to be organized. For every part it
   then states what is implemented today, and what is missing. Target design
   is not a promise that code exists.

2. **Mark status explicitly.** Every part that is not plainly implemented gets
   a status marker, at the start of its paragraph or section:

   * **Implemented** — works in the code base today.
   * **Partial** — some pieces exist, others are missing.
   * **Planned** — only the design exists, nothing is implemented.

   Do not write planned behaviour in the present tense, and do not put design
   decisions under "Current implementation".

3. **Every gap is listed.** Each page has a *Missing parts and issues* section
   (or equivalent) that lists everything the target design needs and the code
   does not yet provide. Each item links to a GitHub issue, for example
   `spinta#1489` or `katalogas#2302`. If no issue exists, say *No issue yet*.
   Do not leave gaps only implied in the prose.

4. **Verify against the code.** Current-implementation claims must match the
   code at the time of writing. Name the module, class or function that does
   the work (for example `spinta/cli/sync.py`, `DEFAULT_DATA_PATH` in
   `spinta/core/config.py`). If the code and the target design disagree, say
   so explicitly instead of smoothing it over.

5. **Keep the C4 layering.** `index.rst` organizes the docs by the C4 model:
   software system (system context), containers (applications and data
   stores), components (modules within a container) and code. A page sits at
   one level and links to its parent and children instead of repeating them.

6. **One source of truth per topic.** Do not copy a description into several
   pages. Describe a concept once and link to it with `:doc:` or `:ref:`.
   Pages in `sync/` describe their target in detail; `sync.rst` only gives the
   overview and status table.

7. **Keep history separate.** Decision history and renamed concepts go into a
   *History* section, not into the target or current-state text.

## How the directory is organized

| Path | Role |
| --- | --- |
| `index.rst` | Entry point: system context, containers, components, code layout. |
| `c4-diagrams.rst`, `uml-diagram.rst`, `wired-together.rst` | Diagrams and how the parts connect. |
| `concepts/` | Shared vocabulary (component, node, metadata). |
| `configuration/` | Config reading, `Context`, `Store`. |
| `multiple-dispatch/` | Command and ufunc dispatch. |
| `user-interfaces/` | CLI and HTTP API. |
| `manifests/` | Manifest formats, logical and physical layers. |
| `data-access/` | Backends, internal data store, external sources, changes, pagination. |
| `auth/`, `access-log/` | Authorization server and resource server; access log. |
| `keymap/` | Keymap implementations and keymap sync. |
| `sync.rst`, `sync/` | `spinta sync` — target design, status table and one page per target. |

When adding a page, put it in the subdirectory of its component and add it to
the nearest `toctree`. A new top-level topic also needs an entry in
`index.rst`.

## Page structure

A page for a part that is not fully implemented follows this order. Omit a
section only when it does not apply.

1. Title, with `.. default-role:: literal` at the top of the file.
2. Introduction: what the part is, where it sits in C4, and its overall status.
   A short status line helps, for example:

   ```
   Target: ``spinta sync manifests``. Status: **Partial**. Parent overview:
   :doc:`/contrib/architecture/sync`.
   ```

3. **Target architecture** (or the design sections, for example *Element*,
   *Location*, *Scheduling*), each part marked with its status.
4. **Current implementation** — what exists, with module paths and the steps
   the code performs.
5. **Missing parts and issues** — every gap, with its issue link.
6. **See also** — links to related pages.

## reStructuredText conventions

* Page title uses overline and underline with `#`. Level 2 sections use `*`,
  level 3 uses `=`. Do not skip levels.
* Commands, paths, option names and code use double backticks, for example
  ``spinta sync``, ``spinta/cli/sync.py``.
* Cross-references: `:doc:` with an absolute path from the docs root
  (`/contrib/architecture/sync`) or a relative path inside the subtree.
  Use `:ref:` labels (for example `contrib-containers`) for components.
* Issues are written as `spinta#N` or `katalogas#N`. Keep the form consistent
  with the existing pages.

## Keeping the docs current

* When code changes a status, update the page status, the status table in the
  parent page and the *Missing parts and issues* list in the same change.
* When an issue is closed, move its item to *Current implementation* or remove
  it from the gap list. Do not leave a closed issue marked as missing.
* Before finishing an edit, build the docs with Sphinx and check for new
  warnings (for example `sphinx-build -b html docs/en <output-dir>` from the
  `nix develop path:.` shell). Use an output directory inside the repository.
