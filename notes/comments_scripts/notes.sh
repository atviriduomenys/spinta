# paleisti šitą komandą, kad pridėtų .gitattributes failą, kad nesugadintų failų galūnių ir nebūtų bereikalingai
# išpūstas diff'as
# tik rc direktorijoj, kad nedarytų įtakos kitiems failams.
echo "* text=auto" > .gitattributes

# export DSA_DIR=some/directory

export scripts_dir="notes/comments_scripts/"

python "$scripts_dir"remove_spaces_column_names.py $DSA_DIR
python "$scripts_dir"quotation_marks_fix.py $DSA_DIR

python "$scripts_dir"bring_back_language_tags.py $DSA_DIR

python "$scripts_dir"remove_part.py $DSA_DIR

# todo adjust levels
# šitas dar blogai veikia, reikėtų pataisyt
python "$scripts_dir"remove_ref_with_dot.py $DSA_DIR


python "$scripts_dir"remove_language_tag_nested.py $DSA_DIR


python "$scripts_dir"remove_part_with_dot_prepare.py $DSA_DIR

