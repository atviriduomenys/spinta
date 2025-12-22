# Goal
Enable Lithuanian (LT) language support and add a multi-file "Setup" documentation structure supporting both Docker and OS installation methods.

# Technical Approach
1.  **Configuration (`docs/conf.py`)**:
    *   Add `setup(app)` hook to define `en` and `lt` tags based on `app.config.language`.
    *   Configure `locale_dirs` for translations.
2.  **Tooling**: Add `sphinx-intl` to `docs/requirements.in`.
3.  **Content Structure (`docs/manual/setup/`)**:
    *   Create directory `docs/manual/setup/`.
    *   Update `docs/manual/setup/index.md` to use a `toctree` directive.
    *   Create `docs/manual/setup/docker.md` and `docs/manual/setup/os.md` with conditional content (EN: placeholder, LT: content).
4.  **Integration**:
    *   Update `docs/index.rst` to include `manual/setup/index`.

# Work Breakdown
- [x] Add `sphinx-intl` to `docs/requirements.in` and update `docs/requirements.txt`.
- [x] Update `docs/conf.py`:
    - [x] Set `locale_dirs = ['locale/']`.
    - [x] Add `setup(app)` function to inject `tags.add(app.config.language)`.
- [x] Create directory `docs/manual/setup/`.
- [x] Update `docs/manual/setup/index.md` to use a conditional `toctree`.
- [x] Create `docs/manual/setup/docker.md` with conditional content.
- [x] Create `docs/manual/setup/os.md` with conditional content.
- [x] Update `docs/index.rst` to include `manual/setup/index` in the `Manual` toctree.
- [x] Run `sphinx-build -b gettext . _build/gettext` inside `docs/` to generate POT templates (verifies config).
- [x] Run `sphinx-intl update -p _build/gettext -l lt` inside `docs/` to initialize locale structure.
- [x] Run `sphinx-autobuild` to preview changes locally.
- [x] Run linter to check new files.
- [x] get userconfirmation that it works
- [x] Commit changes
