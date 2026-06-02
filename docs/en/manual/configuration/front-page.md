# Front page configuration

Spinta's HTML pages (all pages that extend the `base.html` template) display
a notice at the top — for example, a message telling users that the platform
is under active development, or any other piece of information that should be
visible everywhere. This notice is configurable.

## Configuration option

The notice text is stored in the `texts.front_page_warning` option. The value
is read from your `config.yml`; if it is not set there, Spinta falls back to
the default defined in `spinta/config.py`.

```yaml
texts:
  front_page_warning: |
    **Heads up!** The platform is currently under active development.
```

## Markdown syntax

The notice is written in [Markdown](https://commonmark.org/help/). It is
converted to HTML and then sanitized for safety, so values coming from
external or untrusted sources are safe to use — any dangerous HTML
(`<script>`, `onclick` attributes, etc.) is stripped before the page is
rendered.

Common Markdown elements that work:

| Markdown                              | Renders as              |
| ------------------------------------- | ----------------------- |
| `**text**`                            | **bold** text           |
| `*text*`                              | *italic* text           |
| `[label](https://example.com)`        | a link                  |
| `# Heading` … `###### Heading`        | H1–H6 headings          |

## Allowed HTML tags

After Markdown conversion, the result is filtered by
[bleach](https://bleach.readthedocs.io/). Only the following HTML tags are
allowed through:

- `p`, `br`
- `strong`, `em`
- `a` (with `href`, `title`, `target`, `rel` attributes)
- `h1`, `h2`, `h3`, `h4`, `h5`, `h6`

Anything else is stripped, keeping only the inner text. This means lists
(`- item`), block quotes (`> text`) and code spans (`` `code` ``) lose their
HTML structure after filtering, even though the text content survives. If
you need a broader set of tags, you can extend the allow-list in
`spinta/formats/html/helpers.py` (constants `MARKDOWN_ALLOWED_TAGS` and
`MARKDOWN_ALLOWED_ATTRS`).

## Example

In `config.yml`:

```yaml
texts:
  front_page_warning: |
    **Heads up!** The [data storage](https://data.gov.lt/page/saugykla) is
    currently under active development. Please report any issues to
    [atviriduomenys@vssa.lt](mailto:atviriduomenys@vssa.lt).
```

Rendered HTML:

```html
<div class="warning">
  <p>
    <strong>Heads up!</strong> The
    <a href="https://data.gov.lt/page/saugykla">data storage</a> is
    currently under active development. Please report any issues to
    <a href="mailto:atviriduomenys@vssa.lt">atviriduomenys@vssa.lt</a>.
  </p>
</div>
```

## Default value

If `texts.front_page_warning` is not set in your configuration, the default
from `spinta/config.py` is used — a message about active development with a
link to the project documentation. To remove the notice entirely, set the
option to an empty string in `config.yml`:

```yaml
texts:
  front_page_warning: ""
```

## Implementation notes

- The notice text reaches the template via the `get_front_page_warning()`
  helper (`spinta/formats/html/helpers.py`).
- Markdown → HTML conversion and sanitization are performed by the
  `markdown` Jinja2 filter (also defined in `helpers.py`). In templates it
  is used as: `{{ front_page_warning | markdown }}`.
- The filter returns a `markupsafe.Markup` object, so Jinja2 does not
  re-escape the result — no extra `| safe` is needed.
- The filter is general-purpose — it can be reused in any other template
  that needs Markdown rendering.
