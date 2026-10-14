# Feature specifications

A specification describes one feature of Spinta for those who build and
maintain it: what it does, from what and how. User documentation is in
`docs/en` and `docs/lt`, why a decision was made is in an ADR (`docs/adr`), and
what changed for users is in `CHANGES.rst`.

- **A specification describes the final state, without history.** How it used
  to be and why it changed belongs in an ADR or the commit history.
- **One feature, one file**, named after the feature (`udts-oas.md`).
- **A specification points at the code and tests** implementing the feature,
  and at the ADRs that shape it.
- **Do not repeat user documentation**, link to it instead.

```{toctree}
:maxdepth: 1

udts-oas
```
