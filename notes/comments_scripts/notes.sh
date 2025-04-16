# paleisti šitą komandą, kad pridėtų .gitattributes failą, kad nesugadintų failų galūnių ir nebūtų bereikalingai
# išpūstas diff'as
# tik rc direktorijoj, kad nedarytų įtakos kitiems failams.
echo "* text=auto" > .gitattributes

# do export DSA_DIR=some/directory

dsa_dir="$DSA_DIR"

python remove_spaces_column_names.py
python quotation_marks_fix.py dsa_dir
python bring_back_language_tags.py dsa_dir
python remove_part.py dsa_dir
python remove_ref_with_dot.py dsa_dir
python remove_part_with_dot_prepare.py dsa_dir
