#!/bin/bash

base_dir="$PWD"

register_dir="TAAR"

result_dir="${register_dir}_result_new"
result_dir_old="${register_dir}_result_old"
mkdir "$base_dir/xsds/$result_dir/"
mkdir "$base_dir/xsds/$result_dir/csv"
mkdir "$base_dir/xsds/$result_dir_old/"
mkdir "$base_dir/xsds/$result_dir_old/csv"

# Specify the desired extension for the output files
output_extension=".xlsx"

# Iterate over each file in the input directory
for file in "$base_dir/xsds/$register_dir/"*; do
    # Extract the file name without extension
    filename=$(basename "$file" | cut -d. -f1)

    # Generate the output file name by appending the new extension
    output_file="dsa_$filename$output_extension"

    # Run your command to process the file and output to the new file name
    spinta copy -o "xsds/$result_dir/$output_file" "xsd2+file://$file"
done

# Iterate over each file in the input directory
for file in "$base_dir/xsds/$register_dir/"*; do
    # Extract the file name without extension
    filename=$(basename "$file" | cut -d. -f1)

    # Generate the output file name by appending the new extension
    output_file="dsa_$filename$output_extension"

    # Run your command to process the file and output to the new file name
    spinta copy -o "xsds/$result_dir_old/$output_file" "$file"
done

# Specify the desired extension for the output files
output_extension=".csv"

# Iterate over each file in the input directory
for file in "$base_dir/xsds/$register_dir/"*; do
    # Extract the file name without extension
    filename=$(basename "$file" | cut -d. -f1)

    # Generate the output file name by appending the new extension
    output_file="dsa_$filename$output_extension"

    # Run your command to process the file and output to the new file name
    spinta copy -o "xsds/$result_dir/csv/$output_file" "xsd2+file://$file"
done

# Iterate over each file in the input directory
for file in "$base_dir/xsds/$register_dir/"*; do
    # Extract the file name without extension
    filename=$(basename "$file" | cut -d. -f1)

    # Generate the output file name by appending the new extension
    output_file="dsa_$filename$output_extension"

    # Run your command to process the file and output to the new file name
    spinta copy -o "xsds/$result_dir_old/csv/$output_file" "$file"
done
