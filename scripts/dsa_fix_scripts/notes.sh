# paleisti šitą komandą, kad pridėtų .gitattributes failą, kad nesugadintų failų galūnių ir nebūtų bereikalingai
# išpūstas diff'as
# tik rc direktorijoj, kad nedarytų įtakos kitiems failams.
echo "* text=auto" > .gitattributes

# export DSA_DIR=some/directory

export scripts_dir="scripts/dsa_fix_scripts/"

# applied:
python "$scripts_dir"remove_spaces_column_names.py $DSA_DIR
python "$scripts_dir"quotation_marks_fix.py $DSA_DIR
python "$scripts_dir"bring_back_language_tags.py $DSA_DIR
# bring back nested enum https://github.com/atviriduomenys/spinta/issues/540
python "$scripts_dir"bring_back_enum_nested.py $DSA_DIR
python "$scripts_dir"trim_csv_spaces.py $DSA_DIR

python "$scripts_dir"remove_part_997.py $DSA_DIR
python "$scripts_dir"remove_underscore_property_name_963.py $DSA_DIR
python "$scripts_dir"remove_money_type_40.py $DSA_DIR

python "$scripts_dir"remove_ref_with_dot_981.py $DSA_DIR
level: 1
python "$scripts_dir"remove_ref_with_dot_prepare_981.py $DSA_DIR
level: 1

python "$scripts_dir"remove_getone_getall_927.py $DSA_DIR

python "$scripts_dir"remove_ref_required_1313.py $DSA_DIR


python "$scripts_dir"remove_language_tag_nested_1339.py $DSA_DIR

# to apply:

# todo

virsis užsiciklina



#  add array above [] - actually, not necessarily, it works as it is


rasti kažkokį (gal laikiną) sprendimą šitai problemai: https://github.com/ivpk/dsa/discussions/70



,,,,,dokumentai[],backref,"/datasets/gov/rc/gr_ws/n605_isplestinis_duomenu_rinkinys/AsmensDokumentas[nr]"
Kai yra tokia situacija, tai čia gaunasi neaiškumas, ar tas [nr] nurodo pirminį raktą, per kurį jungiami modeliai, ar ref, kuriam yra šitas backref



Vladimirui: `jei bus klaida Model 'X' does not contain any suitable properties for backref` tai pridėjus ref reikėtų pridėti komentarą

,,,,,asmens_objektai1,ref,/datasets/gov/rc/ntr_ws/n200_nt_objektu_ir_teisiu_sarasas_pagal_koda_arba_id/AsmensObjektas,,,,develop,,private,,,,
,,,,,,comment,property,,"delete(property: ""asmens_objektai1"")",,,public,,https://github.com/atviriduomenys/spinta/issues/1314,,,

# asset_type.id.name@lt change everywhere to asset_type.name@lt - ar dar reikia?
