# Autentifikacija ir autorizacija

Spinta Agentas naudoja [OAuth 2.0 Client Credentials](https://ivpk.github.io/uapi/#section/Authorization "UAPI autorizacija")
srautą prieigos kontrolei — tas pats standartas, kurį nustato UAPI specifikacija.

## Autorizacijos valdymo pasirinkimas

Yra du būdai valdyti autorizaciją:

| Būdas | Kada naudoti | Kas valdo |
|-------|-------------|-----------|
| **Vidinis AM** (integruotas į Spintą) | Lokalus testavimas | Institucija |
| **Išorinis AM** (Gravitee Access Manager) | Produkcinė aplinka | VSSA |

:::{note}
Testavimo metu pakanka vidinio AM. Į produkcinę aplinką pereinant, autentifikacija
bus valdoma per Gravitee — institucija nebeturi kurti ar valdyti klientų pati.
:::

## Vidinis autorizacijos valdymas (testavimui)

Spinta turi integruotą OAuth serverį, kuris leidžia sukurti klientus lokaliam
testavimui. Šie klientai saugomi kaip YAML failai agento konfigūracijos kataloge.

### Kliento sukūrimas

Kliento sukūrimas atliekamas `spinta` naudotojo teisėmis:

```bash
sudo -Hsu spinta
cd
export SPINTA_CONFIG=/opt/spinta/config.yml
```

```bash
env/bin/spinta client add -n <kliento-pavadinimas> --scope - << 'EOF'
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
EOF
```

Gautas atsakymas:

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
:::

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
reikalaujančios autentifikacijos), kliento faile reikia nurodyti `backends` sekciją:

```bash
cat /opt/spinta/config/clients/id/f4/0e/76b7-f3f4-4a4c-b2e9-c03a147d65f9.yml
```

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
