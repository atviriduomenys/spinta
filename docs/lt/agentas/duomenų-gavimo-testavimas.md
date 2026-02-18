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

Toliau siųskite užklausą į serverį:

```bash
curl --location 'https://example.com/datasets/gov/vssa/demo/rctest/Country?action_type=%221%22&caller_code=%221%22&parameters=%22%22&time=%221%22&signature=%221%22' \
  --header "$AUTH" | jq '.'
```

Turėtumėte gauti tokį atsakymą:

```
{
  "_data": [
    {
      "_type": "datasets/gov/vssa/demo/rctest/Country",
      "_id": "88ea6c29-57a3-4dda-aa1b-ab550972bee4",
      "_revision": null,
      "_base": null,
      "id": "2",
      "title": "Lietuva"
    },
    {
      "_type": "datasets/gov/vssa/demo/rctest/Country",
      "_id": "967247a7-4d6a-478f-8675-7a6aee530c63",
      "_revision": null,
      "_base": null,
      "id": "1",
      "title": "Lorem"
    }
  ]
}
```
