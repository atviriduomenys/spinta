# paleisti šitą komandą, kad pridėtų .gitattributes failą, kad nesugadintų failų galūnių ir nebūtų bereikalingai
# išpūstas diff'as
# tik rc direktorijoj, kad nedarytų įtakos kitiems failams.
echo "* text=auto" > .gitattributes

# export DSA_DIR=some/directory

export scripts_dir="notes/comments_scripts/"

# applied:
python "$scripts_dir"remove_spaces_column_names.py $DSA_DIR
python "$scripts_dir"quotation_marks_fix.py $DSA_DIR
python "$scripts_dir"bring_back_language_tags.py $DSA_DIR
# bring back nested enum https://github.com/atviriduomenys/spinta/issues/540
python "$scripts_dir"bring_back_enum_nested.py $DSA_DIR
python "$scripts_dir"trim_csv_spaces.py $DSA_DIR


# to apply:


# seems to be working
python "$scripts_dir"remove_part.py $DSA_DIR

# todo adjust levels

python "$scripts_dir"remove_ref_with_dot.py $DSA_DIR

# asset_type.id.name@lt change everywhere to asset_type.name@lt

python "$scripts_dir"remove_money_type.py $DSA_DIR

# needs checking
python "$scripts_dir"remove_ref_with_dot_prepare.py $DSA_DIR


todo
 add array above []
