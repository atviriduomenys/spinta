# Autentifikacija ir autorizacija

Spinta Agentas naudoja [OAuth 2.0 Client Credentials](https://ivpk.github.io/uapi/#section/Authorization "UAPI autorizacija")
srautą prieigos kontrolei — tas pats standartas, kurį nustato UAPI specifikacija.

:::{note}
Testavimo metu pakanka lokalaus AM. Į produkcinę aplinką pereinant, autentifikacija
bus valdoma per Gravitee — institucija nebeturi kurti ar valdyti klientų pati.
:::

## Testo kliento sukūrimas

Norint ištestuoti duomenų gavimą, reikia sukurti OAuth klientą su reikalingais leidimais.
Komandos skiriasi priklausomai nuo diegimo būdo:

**Docker diegimas:**

```bash
OUTPUT=$(docker exec -i spinta poetry run spinta client add -n test --scope - << 'EOF'
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
EOF
) && \
FILE_PATH=$(echo "$OUTPUT" | grep -A2 "saved to:" | tail -1 | xargs) && \
SECRET=$(echo "$OUTPUT" | grep -A2 "Client secret:" | tail -1 | xargs)
```

Patikrinkite gautą atsakymą:

```bash
echo "$OUTPUT"
```

**OS diegimas:**

```bash
sudo -Hsu spinta
cd
export SPINTA_CONFIG=/opt/spinta/config.yml
```

```bash
env/bin/spinta client add -n test --scope - << 'EOF'
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
EOF
```

Gautas atsakymas turėtų atrodyti panašiai į:

```
New client created and saved to:

    /opt/spinta/config/clients/id/f4/0e/76b7-f3f4-4a4c-b2e9-c03a147d65f9.yml

Client secret:

    wjhl5sKB0YkE994yXue8rX0E-dQadKcF

Remember this client secret, because only a secure hash of
client secret will be stored in the config file.
```

:::{caution}
Išsisaugokite gautą `secret` — daugiau jo pamatyti nebegalėsite. Jis saugomas
tik kaip hash'as konfigūracijos faile.

Docker diegimo atveju išsaugotą `secret` galite patikrinti:

```bash
echo $SECRET
```

:::

## Autorizacijos valdymo pasirinkimas

Yra du būdai valdyti autorizaciją:

| Būdas | Kada naudoti | Kas valdo |
|-------|-------------|-----------|
| **Lokalus AM** (integruotas į Spintą) | Lokalus testavimas | Institucija |
| **Nutolęs AM** (Gravitee Access Manager) | Test/Produkcinė aplinka | VSSA |

## Lokalus autorizacijos valdymas (testavimui)

Spinta turi integruotą OAuth serverį, kuris leidžia sukurti klientus lokaliam
testavimui. Šie klientai saugomi kaip YAML failai agento konfigūracijos kataloge.

### Kliento pavadinimas ir UUID

Sukūrus klientą, konfigūracijos faile matomi **du identifikatoriai**:

```yaml
client_id: f40e76b7-f3f4-4a4c-b2e9-c03a147d65f9   # UUID — failo saugojimui
client_name: test                                    # Pavadinimas — autentifikacijai
```

:::{important}
Autentifikacijai (token gavimui) naudojamas **kliento pavadinimas** (`client_name`),
o ne UUID. Spinta nuskaito visus klientų failus ir ieško pagal pavadinimą.

```bash
# ✅ Teisingai — naudoti pavadinimą:
curl -u "test:SECRET" ...

# ❌ Neteisingai — naudoti UUID:
curl -u "f40e76b7-f3f4-4a4c-b2e9-c03a147d65f9:SECRET" ...
```

Naudojant UUID gausite klaidą: `{"error": "invalid_client"}`
:::

### Kliento siejimas su šaltiniu

Jei klientas turi turėti prieigą prie konkretaus šaltinio (pvz., SOAP paslaugos
reikalaujančios autentifikacijos), kliento faile reikia nurodyti `backends` sekciją.

**Docker diegimas:**

```bash
docker exec spinta cat $FILE_PATH
```

```bash
docker exec -i spinta bash -c "sed -i 's/backends: {}/backends:\n  get_data:\n    sub: MTAwMQ==/' $FILE_PATH"
```

**OS diegimas:**

```bash
cat /opt/spinta/config/clients/id/f4/0e/76b7-f3f4-4a4c-b2e9-c03a147d65f9.yml
```

Kliento faile pridėkite `backends` sekciją:

```yaml
client_id: f40e76b7-f3f4-4a4c-b2e9-c03a147d65f9
client_name: test
client_secret_hash: pbkdf2$sha256$...
scopes:
  - uapi:/:getone
  - uapi:/:getall
  - uapi:/:search
  - uapi:/:changes
backends:
  get_data:
    sub: MTAwMQ==   # base64 koduotas šaltinio kliento identifikatorius
```

### Leidimų (scopes) aprašas

| Leidimas | Paskirtis |
|----------|-----------|
| `uapi:/:getone` | Gauti vieną įrašą pagal ID |
| `uapi:/:getall` | Gauti visus įrašus (sąrašas) |
| `uapi:/:search` | Filtruoti ir ieškoti įrašų |
| `uapi:/:changes` | Gauti pakeitimų istoriją |

Daugiau informacijos apie leidimus: [UAPI — Authorization](https://ivpk.github.io/uapi/#section/Authorization/Scope "UAPI scopes")

## Prieigos žetono gavimas

Turint sukurtą klientą, prieigos žetoną galima gauti taip:

```bash
SERVER=http://localhost:8000
CLIENT=test      # kliento PAVADINIMAS, ne UUID
SECRET=secret    # gautas kuriant klientą

TOKEN=$(
  curl -sS -f \
    -u "$CLIENT:$SECRET" \
    -d "grant_type=client_credentials" \
    -d "scope=uapi:/:getall uapi:/:getone uapi:/:search uapi:/:changes" \
    "$SERVER/auth/token" \
  | jq -r .access_token
)
```

Patikrinkite ar žetonas gautas:

```bash
echo $TOKEN
```

Žetonas naudojamas visose tolesnėse užklausose:

```bash
curl -s http://localhost:8000/version \
  -H "Authorization: Bearer $TOKEN"
```


# Nutolusio (Gravitee) AM viešųjų raktų sukėlimas ir konfigūravimas

`Spinta` automatiškai įkelia visus galimus viešuosius raktus JWT žetonų tikrinimui iš `config.yml` aprašytų vietų. Raktai parenkami iš šių šaltinių prioritetų tvarka:

1. **Konfigūracijoje nustatyti raktai** (`token_validation_key`)

   Jei `token_validation_key` yra nurodytas `config.yml` faile, visi jame esantys raktai bus naudojami. Galima nurodyti vieną raktą arba kelis raktus sąraše `keys`.

2. **URL atsisiunčiami raktai** (`token_validation_keys_download_url`)
   
   Jei konfigūracijoje nėra raktų, galima nurodyti OAuth serverio URL, iš kurio raktai bus atsisiųsti. Atsisiųsti raktai paprastai saugomi lokaliame faile (`downloaded_public_keys_file`) tolimesniam naudojimui.

## Konfigūracijos parametrai

```yaml
token_validation_key:  > # vienas raktas arba raktų sąrašas
  {
    "keys": [
      {
        "kid": "rotation-1",
        "kty": "RSA",
        "alg": "RS512",
        "use": "sig",
        "n": "oAXjeXtZxiEUI7EcG6uITGCuUHmMQxMdTuSkQMaijmX0R1xSN-sQOgrunTqzldGWYhn4CQXmE34TgoZs2l6pZKNEyzap5IstPAUTFfHamyLka-xBwVRpCJaM_ZY9dEhzn9NUB-mx1ud9_clhmlef0SRQ1E5N_oU9wA_Hgd6hdnRzzTDJzmueF_03fEEf27fd69qzPZerOO7E9ytHJm0RpTF-50MGDL9pJaomAry_m0cw66DRd8rwqE-MiSg1xo02YWYIbaNA13K7jO33lW3iqgLdmtiBvX7qoNhEXC5H_umLvd5hgETGVemFcdFgL0Xnj85uk3puiVMsYXqmzHNxdw",
        "e": "AQAB"
      },
      {
        "kty": "RSA",
        "n": "l9oSzRInpwJLwsFEs80JQlPyf0k-AqvOef2H-1JpNeaivltEzA_hSX6SSEAm7vciOVOuxBJ0iGr7s0_wY0fKEJ4aFiYHR46zpHT_o0iZxrLwIKJugqDEE96mEPK-o5gVRqs-QJXDmaHzAkVntQRMP6GzKHy5Q6ZZQJWwKg_eTSGnGph34T7PUfSNF50G7qflqmWBiVW7qaNFKbgmB_7be-WZ9mbMVwSMMQwo_aahJqI1ZndKXgYGoEabwgSuJOQrAQbvVOyOHTj0ku-FNo8kb9dVeqi0F1qCvs6SAhCzk7qT215xalWIpX8BI1ZFnCv--6VMqIKEgTtjaaDh3V25SQ",
        "e": "AQAB"
      }
    ]
  }
token_validation_keys_download_url: https://<auth-serverio-adresas> # URL, iš kurio galima atsisiųsti viešuosius raktus
downloaded_public_keys_file: "custom-well-knows.json" # vietinis failas atsisiųstiems raktams saugoti. Jei nenurodyta - "downloaded-well-knows.json" bus naudojamas.
```

:::{note}
`token_validation_keys_download_url` negali būti naudojamas kartu su `token_validation_key`, nes abu atlieka tą pačią funkciją. Skirtumas tas, kad `token_validation_keys_download_url` yra dinamiškesnis ir palaiko raktų rotaciją. Dėl šios priežasties siūloma naudoti `token_validation_keys_download_url`. Dėl `token_validation_keys_download_url` kreiptis į VSSA.
:::

Naujus raktus galima parsisiųsti su komanda:

```bash
spinta key download
```

:::{warning}
Jei naudojamas `token_validation_keys_download_url` ir reikalingas raktų rotavimas, papildomai reikia sukonfigūruoti CRON, kuris periodiškai vykdytų komandą `spinta key download` (priklausomai nuo naudojamo auth serverio raktų rotacijos dažnio).
:::

Pavyzdys CRON konfigūracijos:

```bash
# Redaguokite crontab:
crontab -e

# Pridėkite šią eilutę:
0 3 * * 1 /usr/bin/env bash -c 'cd /path/to/spinta/project && spinta key download >> /var/log/auth.log 2>&1'
```

Šiame pavyzdyje raktai bus atnaujinami iš auth serverio **kas savaitę pirmadienį 03:00 val.**

Sau tinkama periodiškumą galima nustatyti pagal https://crontab.guru/
