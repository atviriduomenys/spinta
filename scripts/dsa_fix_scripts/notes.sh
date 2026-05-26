# Disclaimer - šie scriptai rašyti su LLM'ų pagalba ir mažai peržiūrėti bei modifikuoti, todėl gali nebūtinai
# atitikti gerąsias praktikas

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

python "$scripts_dir"remove_ref_with_dot_prepare_981.py $DSA_DIR


python "$scripts_dir"remove_getone_getall_927.py $DSA_DIR

python "$scripts_dir"remove_ref_required_1313.py $DSA_DIR


python "$scripts_dir"remove_language_tag_nested_1339.py $DSA_DIR

