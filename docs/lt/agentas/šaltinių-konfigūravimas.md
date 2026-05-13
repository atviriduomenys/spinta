# Šaltinių konfigūravimas

Tam, kad Agentas galėtų pasiekti duomenis ir teikti juos UDTS formatu, reikia nurodyti, kokie duomenys bus teikiami ir kaip juos pasiekti. Tai daroma naudojantis duomenų struktūros aprašais (DSA). Šiuo atveju, mums reikės Šaltinio duomenų struktūros aprašų (ŠDSA).

Kaip nurodyta konfigūracijos faile, turite pateikti duomenų struktūros aprašą, kurio pagrindu veiks agentas.

Spinta palaikomi šaltiniai:

- SQL
- WSDL/SOAP
- XML
- JSON

## WSDL ir SOAP šaltiniai

WSDL ir SOAP šaltinio struktūros parengimas aprašytas čia:

[Duomenų šaltiniai - DSA](https://ivpk.github.io/dsa/draft/saltiniai.html#wsdl "Duomenų šaltiniai - DSA")

Pasikeiskite aktyvų naudotoją ir katalogą:

```bash
sudo -Hsu spinta
cd
```

Struktūros aprašo, skirto WSDL duomenims gauti, sudarymo pavyzdys:

```bash
cat > manifest.csv << 'EOF'
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/gov/vssa/demo/rctest,,,,,dataset,,,,,,,,,,,
,,rc_wsdl,,,,wsdl,,https://test-data.data.gov.lt/api/v1/rc/get-data/?wsdl,,,,,,,,,
,,get_data,,,,soap,,Get.GetPort.GetPort.GetData,wsdl(rc_wsdl),,,,,,,,
,,,,,,param,action_type,input/ActionType,input(),,,,,,,,
,,,,,,param,caller_code,input/CallerCode,input(),,,,,,,,
,,,,,,param,end_user_info,input/EndUserInfo,input(),,,,,,,,
,,,,,,param,parameters,input/Parameters,input(),,,,,,,,
,,,,,,param,time,input/Time,input(),,,,,,,,
,,,,,,param,signature,input/Signature,"creds(""sub"").input()",,,,,,,,
,,,,,,param,caller_signature,input/CallerSignature,input(),,,,,,,,
,,,,GetData,,,,/,,,,,,,,,
,,,,,response_code,string,,ResponseCode,,,,,,,,,
,,,,,response_data,string,,ResponseData,base64(),,,,,,,,
 ,,,,,decoded_parameters,string,,DecodedParameters,,,,,,,,,
 ,,,,,action_type,string,,,param(action_type),,,,,,,,
,,,,,end_user_info,string,,,param(end_user_info),,,,,,,,
,,,,,caller_code,string,,,param(caller_code),,,,,,,,
,,,,,parameters,string,,,param(parameters),,,,,,,,
,,,,,time,string,,,param(time),,,,,,,,
,,,,,signature,string,,,param(signature),,,,,,,,
,,,,,caller_signature,string,,,param(caller_signature),,,,,,,,
,,,,,,,,,,,,,,,,,
,,nested_read,,,,dask/xml,,,eval(param(nested_xml)),,,,,,,,
 ,,,,,,param,nested_xml,GetData,read().response_data,,,,,,,,
,,,,Country,,,,countries/countryData,,,,,,,,,
,,,,,id,string,,id,,,,,,,,,
,,,,,title,string,,title,,,,,,,,,
EOF
```
