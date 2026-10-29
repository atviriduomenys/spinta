# Duomenų gavimo testavimas

Žemiau nurodytas pavyzdys, kaip galima ištestuoti duomenų gavimą komandinės eilutės pagalba, bet taip pat galit naudoti ir REST API įrankius, kaip [Postman](https://www.postman.com/ "Postman"), [Insomnia](https://insomnia.rest/ "Insomnia") ar panašius.

**Kompiuteryje, kuriame norime gauti duomenis**, pirmiausiai nurodom kliento duomenis, įskaitant norimus leidimus (jei testuojame iš kito kompiuterio, nei sudiegėme agentą, vietoje localhost nurodome agento URL arba IP adresą):

```bash
SERVER=http://localhost:8000
CLIENT=test      # kliento PAVADINIMAS (nurodytas su -n flag), NE UUID
SECRET=secret    # pakeiskite į jūsų turimą secret (rodomas kuriant klientą)
SCOPES="
  uapi:/:getone
  uapi:/:getall
  uapi:/:search
  uapi:/:changes
"
```

Tada reikia paleisti komandą, kurios pagalba gautome autorizacijos žetoną:

```bash
TOKEN=$(
  curl -sS -f \
    -u "$CLIENT:$SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=$SCOPES" \
    "$SERVER/auth/token" \
  | jq -r .access_token
)
```

Pasitikrinkite, ar gavote žetoną:

```bash
AUTH="Authorization: Bearer $TOKEN"
echo "$AUTH"
```

Turėtumėte gauti kažką panašaus į:

```bash
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxmiIsImtpZCI6IlVPb1M0dXU5Q2dnMTNsTV9sV3pxcklQUFUzYVdPd2hjODk1UWFEU21QUGMifQ.eyJpc3MiOiJodHRwczovL2V4YW1wbGUuY29tLyIsInN1YiI6ImY0MGU3NmI3LWYzZjQtNGE0Yy1iMmU5LWMwM2ExNDdkNjVmOSIsImF1ZCI6ImY0MGU3NmI3LWYzZjQtNGE0Yy1iMmU5LWMwM2ExNDdkNjVmOSIsImlhdCI6MTc2MzgzODMyOCwiZXhwIjoxNzY0NzAyMzI4LCJzY29wZSI6InVhcGk6LzpjaGFuZ2VzIHVhcGk6LzpnZXRhbGwgdWFwaTovOmdldG9uZSB1YXBpOi86c2VhcmNoIiwianRpIjoiZmM3NmQ5NmYtNmFjZi00NGE4LWExY2UtYjcwZTA1ZjIzYjg1In0.jnSCaVSO3iDap61ifFDnvKY2eurnUUT-bdbfLRZTBrhSPqNgAaoCLogtODhQsOrPfC8jCcqX3GabuynyMfURKfEY466L9CTwgKNVAc4JkAkQcRhLuER92hsK862shVPOJLVugIaJ1eGPMCmiuPXGeIwEpr7idR_v0we8anbvPdCaXKhC0QmSfJS7f0OwmPt9GVJRYVjUo73ZsGZC1VNeIFtG-nA_X0ZaG-KJOry8Coyye3fgJanXKNY9N0LU_U8TPkLZLzgNC8MVYIeTufsSJMP8JZPFOY9t99F5pDok89NhPmjIhBlykpxnerL_SKF4zRJVpzjCUCjiZzHBOUEMLQ
```

:::{note}
Prieš testuojant konkrečius duomenų modelius, rekomenduojama patikrinti bendrą
serviso veikimą naudojant `/version` endpoint:

```bash
curl -s "$SERVER/version" -H "$AUTH" | jq '.'
```

Jei gausite versiją — servisas veikia ir žetonas galioja. Tada galima tęsti.
:::

Toliau siųskite užklausą į serverį:

```bash
curl "$SERVER/datasets/gov/vssa/demo/demo_udts/rctest/Country?_limit=5" -H "$AUTH" | jq '.'
```

Turėtumėte gauti tokį atsakymą:

```
{
  "_data": [
    {
      "_type": "datasets/gov/vssa/demo/demo_udts/rctest/Country",
      "_id": "88ea6c29-57a3-4dda-aa1b-ab550972bee4",
      "_revision": null,
      "_base": null,
      "id": "2",
      "title": "Lietuva"
    },
    {
      "_type": "datasets/gov/vssa/demo/demo_udts/rctest/Country",
      "_id": "967247a7-4d6a-478f-8675-7a6aee530c63",
      "_revision": null,
      "_base": null,
      "id": "1",
      "title": "Lorem"
    }
  ]
}
```

(pilnas-patikrinimas)=
## Pilnas patikrinimas

Lokalus testas (`http://localhost:8000`) parodo, kad agentas veikia. Kad įsitikintumėte, jog
agentas pasiekiamas ir saugus, tuos pačius patikrinimus atlikite **per HTTPS agento adresu**
(pvz., `https://spinta.institucija.lt`), **testavimo ir gamybinio** agento:

```bash
SERVER=https://spinta.institucija.lt
```

Gaukite žetoną ir įsirašykite jį į kintamąjį `AUTH`, kurį naudoja tolesnės komandos:

```bash
TOKEN_URL="$SERVER/auth/token"   # žr. pastabą žemiau
CLIENT=test                      # kliento PAVADINIMAS, ne UUID
SECRET=secret                    # kliento secret

TOKEN=$(
  curl -sS -f \
    -u "$CLIENT:$SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=uapi:/:getall uapi:/:getone uapi:/:search" \
    "$TOKEN_URL" \
  | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo "$AUTH"
```

:::{note}
Tai **pavyzdys**. `TOKEN_URL` priklauso nuo to, kas išduoda žetonus:

- **pats agentas** (lokalus autorizacijos serveris) – `$SERVER/auth/token`, kaip pavyzdyje;
- **nutolęs autorizacijos serveris** (Gravitee AM) – agento `/auth/token` išjungtas, žetonas
  gaunamas iš AM; jo adresą, kliento duomenis ir leidžiamus scope suteikia VSSA.

Kliento kūrimas ir scope aprašyti puslapyje [Autentifikacija ir autorizacija](autentifikacija.md).
:::

**1. Užklausa be žetono** – agentas turi atsisakyti grąžinti duomenis:

```bash
curl -s "$SERVER/datasets/gov/vssa/demo/demo_udts/rctest/Country?_limit=5" | jq '.'
```

```json
{
  "errors": [
    {
      "type": "system",
      "code": "AuthorizedClientsOnly",
      "template": "This resource can only be accessed by an authorized client.",
      "context": {},
      "message": "This resource can only be accessed by an authorized client."
    }
  ]
}
```

Jei be žetono grąžinami duomenys, manifeste duomenims palikta prieinamumo lygio reikšmė
`open` – ją reikia pakeisti (žr. [Apie manifestą](index.md)).

**2. Užklausa su žetonu** – grąžinami duomenys:

```bash
curl -s "$SERVER/datasets/gov/vssa/demo/demo_udts/rctest/Country?_limit=5" -H "$AUTH" | jq '.'
```

Atsakymas – kaip aukščiau pateiktame pavyzdyje.

**3. Užklausa į duomenų rinkinį** – grąžinamas rinkinio modelių sąrašas:

```bash
curl -s "$SERVER/datasets/gov/vssa/demo/demo_udts/rctest" -H "$AUTH" | jq '.'
```

```json
{
  "_data": [
    {
      "name": "datasets/gov/vssa/demo/demo_udts/rctest/Country",
      "title": "Šalys",
      "description": ""
    }
  ]
}
```

Jei rinkinyje yra vidinių rinkinių, jų sąrašą gausite kelio gale pridėję `/:ns`, pvz.,
`"$SERVER/datasets/gov/vssa/demo/demo_udts/rctest/:ns"`.

:::{note}
**DVMS projekto dalyviams.** Šių trijų patikrinimų ekrano nuotraukos (testavimo ir gamybinio
agento) teikiamos projekto galutinėje ataskaitoje (R2.6 punktas).
:::

## Užklausų parametrai testavimo metu

UDTS palaiko [UAPI užklausų parametrus](https://ivpk.github.io/uapi/#section/Query), kurie leidžia filtruoti ir riboti grąžinamus duomenis. Tai ypač naudinga testavimo metu, kai duomenų šaltinyje gali būti daug įrašų.

### `_limit` — įrašų skaičiaus ribojimas

Rekomenduojama visada naudoti `_limit` parametrą testavimo metu, kad būtų grąžinamas tik nedidelis įrašų skaičius:

```bash
curl "$SERVER/datasets/gov/example/MyModel?_limit=5" \
  -H "$AUTH" | jq '.'
```

Atsakyme bus grąžinta ne daugiau kaip 5 įrašai:

```json
{
  "_data": [
    { "_type": "datasets/gov/example/MyModel", "_id": "...", "field": "value" },
    { "_type": "datasets/gov/example/MyModel", "_id": "...", "field": "value" }
  ]
}
```

:::{note}
Jei `_limit` nenurodytas, serveris taiko numatytąją vertę (paprastai 100 įrašų). Didelių duomenų šaltinių atveju tai gali sulėtinti atsakymą arba sukelti timeout klaidą.
:::

### `_limit=1` — greitas patikrinimas

Norėdami tik patikrinti, ar duomenų šaltinis pasiekiamas ir grąžina bent vieną įrašą:

```bash
curl "$SERVER/datasets/gov/example/MyModel?_limit=1" \
  -H "$AUTH" | jq '.'
```

### Puslapiavimas su `_offset`

Jei norite peržiūrėti kitus įrašus (ne pirmuosius):

```bash
# Gauti 5 įrašus pradedant nuo 11-o
curl "$SERVER/datasets/gov/example/MyModel?_limit=5&_offset=10" \
  -H "$AUTH" | jq '.'
```

## Derinimas ir žurnalai

### Žurnalų vietos

| Žurnalas | Komanda / vieta | Turinys |
|----------|----------------|---------|
| SystemD žurnalas | `sudo journalctl -u spinta -xe` | Serviso klaidos, paleidimas, Python exception'ai |
| Realaus laiko stebėjimas | `sudo journalctl -u spinta -f` | Naujausios žurnalo eilutės |
| Prieigos žurnalas | `/opt/spinta/logs/access.log` | HTTP užklausos (atsiranda po pirmos užklausos) |

:::{note}
`access.log` failas sukuriamas tik po **pirmos HTTP užklausos** į servisą —
iškart po paleidimo jo dar nėra.
:::

### Dažniausios klaidos

| Klaida | Galima priežastis | Sprendimas |
|--------|------------------|------------|
| `{"error": "invalid_client"}` | Naudojamas UUID vietoj kliento pavadinimo | Naudoti `client_name` (pvz., `test`), ne UUID |
| `curl: (7) Failed to connect` | Servisas nepasiruošęs arba neveikia | Patikrinti `systemctl status spinta`, palaukti |
| `{"error": "invalid_token"}` | Žetonas pasibaigęs | Gauti naują žetoną |
| `404 Not Found` modeliui | Modelis nėra manifeste | Patikrinti `manifest.csv` turinį |
| `Redis connection failed` | Redis konteineris neveikia | `docker ps`, `docker compose up -d` |
| `No space left on device` | Pilnas diskas | `df -h`, išvalyti vietą |

### Konfigūracijos tikrinimas

Jei servisas nepaleidžiamas, patikrinkite konfigūraciją:

```bash
sudo -u spinta bash -c 'export SPINTA_CONFIG=/opt/spinta/config.yml && \
  /opt/spinta/env/bin/spinta config config backends manifests accesslog'
```

Manifesto tikrinimas:

```bash
sudo -u spinta bash -c 'export SPINTA_CONFIG=/opt/spinta/config.yml && \
  /opt/spinta/env/bin/spinta check'
```
