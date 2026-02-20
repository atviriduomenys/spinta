# Spinta diegimas operacinėje sistemoje

## Python versijos patikrinimas

Daugelis Linux distribucijų ateina su įdiegta Python versija, tačiau reikia įsitikinti, ar distribucijos Python versija yra tinkama:

```bash
python3 --version
```

Jei sisteminė Python versija yra tarp 3.10 ir 3.13 imtinai ir ji jums tinkama, gali būti, kad operacinėje sistemoje nėra Python virtualių aplinkų kūrimo įrankio. Jį galite sudiegti taip:

```bash
sudo apt install python3-venv
```

:::{note}
Jei Python versija yra tarp 3.10 ir 3.13 imtinai bei turite Python virtualių aplinkų kūrimo įrankį, tada galite praleisti sekantį žingsnį, kitu atveju reikia įsidiegti tinkamą Python versiją.
:::

## Tinkamos Python versijos diegimas

Paprasčiausias būdas įdiegti tinkamą Python versiją yra naudojantis **Pyenv** metodu, Ubuntu atveju galite tai padaryti taip:

```bash
sudo apt install -y \
git make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget \
curl llvm libncurses5-dev libncursesw5-dev \
xz-utils tk-dev libffi-dev liblzma-dev
sudo git clone https://github.com/pyenv/pyenv.git /opt/pyenv/
export PYENV_ROOT=/opt/pyenv
/opt/pyenv/bin/pyenv install --list | grep -v - | tail
sudo PYENV_ROOT=/opt/pyenv /opt/pyenv/bin/pyenv install 3.13.9
```

arba pridedant repozitoriją **deadsnake/ppa**:

```bash
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.13 python3.13-venv python3.13-dev build-essential
```

## Spinta diegimas

:::{note}
Atkreipkite dėmesį, kad visos komandos diegiant Spintą turi būti vykdomos Spintos naudotojo teisėmis ir iš Spintos naudotojo namų katalogo /opt/spinta.
:::

Aktyvų naudotoją ir katalogą galite pasikeisti taip:

```bash
sudo -Hsu spinta
cd
```

Agentas veikia Spinta įrankio pagalba, kuriam yra reikalinga Python programavimo kalba. Rekomenduojama visus Python paketus diegti izoliuotoje Python aplinkoje, kurią galima susikurti taip:

```bash
python3 -m venv env
```

### Pyenv

Jei diegėte Python versiją **Pyenv** metodu - nepamirškite nurodyti jūsų naudojamos Python versijos numerio, kuris gali skirtis:

```bash
/opt/pyenv/versions/3.13.9/bin/python -m venv env
```

### Deadsnake/ppa

Jei diegėte python versiją deadsnake/ppa metodu:

```bash
python3.13 -m venv env
```

Toliau diekite Spintą.

DVMS partneriams projekto vystymo metu reikia diegti naujausią pre-release versiją, kadangi joje yra naujausi projekto vystymui ir partnerių darbui atlikti pakeitimai. Spintą diegti rekomenduojama su VSSA patvirtintais paketais:

```bash
env/bin/pip install --require-hashes -r https://raw.githubusercontent.com/atviriduomenys/spinta/refs/heads/master/requirements/spinta-latest-pre.txt
```

Sudiegę, galite patikrinti sudiegtą Spintos versiją:

```bash
env/bin/spinta --version
```

## Spintos Agento konfigūracijos failų paruošimas

<p align="center">
  <img src="../../_static/SOAP-šaltinio-generavimas.png" alt="SOAP šaltinio konfigūravimo pavyzdys">
  <br>
  <em>SOAP šaltinio konfigūravimo pavyzdys</em>
</p>

:::{note}
Spintos Agentą galima konfigūruoti ne vienu būdu ir žemiau pateiktos instrukcijos yra bazinės, kai Agentas konfigūruojamas darbui su SOAP/WSDL šaltiniu (paveikslėlyje atitinka Agentą B).
:::

Spinta yra konfigūruojama konfigūracijos failo pagalba, kurio, pagal nutylėjimą ieškoma aktyviame kataloge. Kur Spinta ieško konfigūracijos failo, galima patikrinti taip:

```bash
env/bin/spinta config config
```

Konfigūracijos failą galima nustatyti aplinkos parametro pagalba:

```bash
export SPINTA_CONFIG=/opt/spinta/config.yml
```

Konfigūracijos failą taip pat galima nurodyti ir Bash konfigūraciniame faile:

```bash
cat >> .bashrc << 'EOF'

export SPINTA_CONFIG=/opt/spinta/config.yml

EOF
```

Kadangi Spintos konfigūracijos failas neegzistuoja, reikia jį sukurti:

```bash
cat > config.yml <<'EOF'
config_path: /opt/spinta/config
env: production

keymaps:
  default:
    type: redis
    dsn: redis://localhost:6379/1

accesslog:
  type: file
  file: /opt/spinta/logs/access.log
EOF
```

### Failo aprašas

| Kintamasis  | Privalomas ar neprivalomas | Aprašas                                                                                                                                            |
| ----------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| config_path | Privalomas                 | Nurodo kelią į direktoriją, kurioje yra įvairūs konfigūracijos duomenys                                                                            |
| env         |                            | Nurodo, ar aplinka testinė, ar vystymo, ar produkcinė. Rekomenduojama diegti su `production`, nes `dev` arba `test` įrašo papildomų konfigūracijų. |
| keymaps     | Privalomas                 | Aprašyta žemiau                                                                                                                                    |
| .default    |                            | Nurodo numatytąjį                                                                                                                                  |
| ..type      |                            | Tipas - `redis`                                                                                                                                    |
| ..dsn       |                            | Nurodo, kaip pasiekti servisą                                                                                                                      |
| accesslog   |                            | Nurodo, kur bus saugomi prieigos žurnalai                                                                                                          |
| .type       |                            | Tipas - ar saugoma faile, ar kitaip                                                                                                                |
| .file       |                            | Jei saugoma faile, nurodo, kuriame faile                                                                                                           |

Prieš testuojant ar konfigūracija veikia, sukuriame reikalingus katalogus:

```bash
#config ir logs katalogai šiuo atveju kuriami atitinkamai pagal config.yml failą
mkdir /opt/spinta/config
mkdir /opt/spinta/logs
#optional jei naudojama sqlite
mkdir /opt/spinta/var
```

### Konfigūracijos tikrinimas

```bash
env/bin/spinta config config backends manifests accesslog
```

:::{note}
Jei jūsų duomenų šaltinis - reliacinė duomenų bazė, patikriname ar Spinta gali prisijungti prie duomenų bazės.

```bash
env/bin/spinta wait 1
```

:::

## Keymap DB

<p align="center">
  <img src="../../_static/keymap-db-os.png" alt="KeymapDB">
  <br>
  <em>Keymap DB</em>
</p>

:::{warning}
Pagal numatytuosius nustatymus naudojama SQLite, tačiau būtina pakeisti į Redis persistent.
:::

Keymap naudojamas susieti išorinius identifikatorius su vidiniais identifikatoriais. Gali būti konfigūruojama.

Spintos agentui nurodome kokį keymap komponentą naudoti ir kaip jį pasiektį per config.yml failą. Pateikiame config.yml failo fragmentą:

```
keymaps:
  default:
    type: redis
    dsn: redis://redis-address:6379/1
```

:::{tip}
Jei neturite `Redis` serviso, rekomenduojame diegti Docker konteineryje.
:::

### Pavyzdinė Redis diegimo instrukcija

Sukurkite docker-compose.yml failą:

```bash
cat > docker-compose.yml << "EOF"
services:
  redis-keymap: # sqlite greitesnė alternatyva
    image: valkey/valkey:9
    restart: always
    command: [ "redis-server", "--appendonly", "yes", "--appendfsync", "always" ] # šie nustatymai užtikrina pilną apsaugojimą nuo duomenų praradimo
    volumes:
      - redis_keymap_data:/data
    ports:
      - "6379:6379"
volumes:
  redis_keymap_data:
EOF
```

:::{caution}
**SVARBU! Redis būtinai turi būti leidžiamas persistent režimu (appendonly:yes ir appendfsync:always parametrai)**

Yra keli persistent režimai (žr. Redis/Valkey dokumentaciją). Numatytasis režimas (appendonly:yes ir appendfsync:always) užtikrina didžiausią duomenų nepraradimo patikimumą, tačiau turi mažiausią greitį naujo rakto kūrimo metu, lyginant su kitais režimais.
:::

Paleiskite dockerį ir patikrinkite:

```bash
docker compose up -d
docker ps
```

Pakeiskite konfigūracijos failo vietą aplinkos kintamojo pagalba:

```bash
export SPINTA_CONFIG=/opt/spinta/config.yml
```

Patikrinkite ar konfigūracijoje ir pateiktame struktūros apraše nėra klaidų.

```bash
env/bin/spinta check
```

:::{warning}
Keymap (Redis) yra kritinis elementas, tad privalote užtikrinti, kad duomenys nebūtų prarasti ar sugadinti, nes tokiu atveju bus pažeistas perduodamų duomenų integralumas.
:::

## Spinta Agento web serverio serviso konfigūravimas

:::{note}
`uvicorn`, `uvloop` ir `httptools` bibliotekos yra įtrauktos į standartinį Spinta
requirements failą, todėl papildomai jų diegti nereikia. Šios komandos reikalingos
tik tuo atveju, jei Spintą diegėte ne iš `spinta-latest-pre.txt` failo:

```bash
env/bin/pip install uvicorn uvloop httptools
```

:::

Atsijunkite nuo Spintos vartotojo

```bash
exit
```

Su `root` teisėmis sukuriame [SystemD](https://systemd.io/ "SystemD") servisą (atkreipkite dėmesį, kad jūsų pasirinkta distribucija gali naudoti kitą servisų valdymo priemonę, tuomet šis pavyzdys netiks):

```bash
cat << 'EOF' | sudo tee /etc/systemd/system/spinta.service
[Unit]
Description=Spinta external WSDL service
After=network.target

[Service]
Type=simple
User=spinta
Group=www-data
WorkingDirectory=/opt/spinta
Environment="SPINTA_CONFIG=/opt/spinta/config.yml"

ExecStart=/opt/spinta/env/bin/spinta \
    -o config=/opt/spinta/config.yml \
    run /opt/spinta/manifest.csv \
    --mode external \
    --host 127.0.0.1 \
    --port 8000

Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

Aktyvuokite Spinta servisą:

```bash
sudo systemctl enable spinta
sudo systemctl daemon-reload
sudo systemctl start spinta
```

Patikrinkite servisą:

```bash
sudo systemctl status spinta
```

:::{note}
Net kai `systemctl status` rodo `active (running)`, Spinta web serveris gali dar kelias sekundes inicializuotis. Norėdami įsitikinti, kad servisas tikrai pasiruošęs priimti užklausas, galite patikrinti `/version` endpoint:

```bash
curl -s http://127.0.0.1:8000/version
```

Jei servisas veikia, gausite atsakymą su versijos informacija, pvz.:

```json
{"implementation":{"name":"Spinta","version":"0.2.dev16"},"uapi":{"version":"0.1.0"},"dsa":{"version":"0.1.0"}}
```

Jei gausite klaidą arba jokio atsakymo — patikrinkite žurnalus: `sudo journalctl -u spinta -xe`
:::

## Kliento sukūrimas

Testo kliento sukūrimas aprašytas skyriuje [Autentifikacija ir autorizacija](../autentifikacija.md#testo-kliento-sukūrimas).
