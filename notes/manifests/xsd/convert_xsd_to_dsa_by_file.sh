#!/bin/bash

# Specify the desired extension for the output files
output_extension=".csv"

# Iterate over each file in the input directory
for file in ./*; do
    # Extract the file name without extension
    filename=$(basename "$file" | cut -d. -f1)

    # Generate the output file name by appending the new extension
    output_file="dsa_$filename$output_extension"

    # Run your command to process the file and output to the new file name
    spinta copy -o "$output_file" "$file"
done
