# Pradinio puslapio pranešimo konfigūravimas

Spinta HTML puslapiuose (visuose, kurie paveldi `base.html` šabloną) viršuje
rodomas įspėjamasis pranešimas — pavyzdžiui, apie tai, kad sistema yra aktyviai
vystoma, arba kitokia svarbi informacija vartotojui. Šio pranešimo tekstą
galima keisti konfigūracijoje.

## Konfigūracijos parametras

Pranešimo tekstas saugomas `texts.front_page_warning` parametre. Reikšmė
nuskaitoma iš `config.yml` failo, o jei ten parametras nenurodytas — naudojama
numatytoji reikšmė iš `spinta/config.py`.

```yaml
texts:
  front_page_warning: |
    **Dėmesio!** Saugykla šiuo metu aktyviai vystoma.
```

## Markdown sintaksė

Pranešimo tekstas rašomas [Markdown](https://commonmark.org/help/) sintakse.
Jis automatiškai paverčiamas į HTML ir filtruojamas dėl saugumo, todėl į
konfigūraciją galima saugiai rašyti ir tekstą iš išorinių šaltinių —
kenkėjiškas HTML (pavyzdžiui, `<script>` arba `onclick` atributai) bus
pašalintas.

Palaikomi tipiniai Markdown elementai:

| Markdown                              | Rezultatas              |
| ------------------------------------- | ----------------------- |
| `**tekstas**`                         | **paryškintas** tekstas |
| `*tekstas*`                           | *pasvirasis* tekstas    |
| `[etiketė](https://pavyzdys.lt)`      | nuoroda                 |
| `# Antraštė` ... `###### Antraštė`    | antraštės (H1–H6)       |

## Leistini HTML elementai

Po Markdown konvertavimo rezultatas saugumo tikslais filtruojamas su
[bleach](https://bleach.readthedocs.io/) biblioteka. Praleidžiami tik šie HTML
tag'ai:

- `p`, `br`
- `strong`, `em`
- `a` (su `href`, `title`, `target`, `rel` atributais)
- `h1`, `h2`, `h3`, `h4`, `h5`, `h6`

Visi kiti HTML tag'ai pašalinami, paliekant tik jų teksto turinį. Tai reiškia,
kad sąrašai (`- punktas`), citatos (`> tekstas`) ir kodo blokai (`` `code` ``)
po filtravimo praranda savo struktūrinį HTML žymėjimą, nors pats tekstas
išlieka. Jei prireiktų papildomų elementų, leistinų tag'ų sąrašą galima
išplėsti faile `spinta/formats/html/helpers.py`
(`MARKDOWN_ALLOWED_TAGS` ir `MARKDOWN_ALLOWED_ATTRS` konstantos).

## Pavyzdys

`config.yml` faile:

```yaml
texts:
  front_page_warning: |
    **Dėmesio!** [Saugykla](https://data.gov.lt/page/saugykla) šiuo metu
    aktyviai vystoma. Apie sutrikimus prašome pranešti
    [atviriduomenys@vssa.lt](mailto:atviriduomenys@vssa.lt).
```

Rezultatas HTML puslapyje:

```html
<div class="warning">
  <p>
    <strong>Dėmesio!</strong>
    <a href="https://data.gov.lt/page/saugykla">Saugykla</a> šiuo metu
    aktyviai vystoma. Apie sutrikimus prašome pranešti
    <a href="mailto:atviriduomenys@vssa.lt">atviriduomenys@vssa.lt</a>.
  </p>
</div>
```

## Numatytoji reikšmė

Jei `texts.front_page_warning` nenurodytas konfigūracijoje, naudojama
numatytoji reikšmė iš `spinta/config.py` — pranešimas apie aktyvų saugyklos
vystymą su nuoroda į projekto dokumentaciją. Norint visiškai pašalinti
pranešimą, `config.yml` faile užtenka nurodyti tuščią reikšmę:

```yaml
texts:
  front_page_warning: ""
```

## Techninės detalės

- Pranešimo tekstas į šabloną patenka per `get_front_page_warning()`
  pagalbinę funkciją (`spinta/formats/html/helpers.py`).
- Markdown → HTML konvertavimą ir saugumo filtravimą atlieka
  `markdown` Jinja2 filtras (taip pat apibrėžtas `helpers.py` faile).
  Šablone naudojama: `{{ front_page_warning | markdown }}`.
- Filtras grąžina `markupsafe.Markup` objektą, todėl Jinja2 nebešifruoja
  rezultato — papildomas `| safe` nereikalingas.
- Filtras yra bendros paskirties — jį galima panaudoti ir kituose
  šablonuose, kuriuose reikia Markdown rendering'o.
