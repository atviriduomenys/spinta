import os
import sys

import pandas as pd
import re

def process_csv_files(directory):
    # Recursively walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                process_csv_file(file_path)

def process_csv_file(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Check if the 'ref' column exists
    if 'ref' in df.columns:
        # Iterate over each row in the DataFrame
        for index, row in df.iterrows():
            ref_value = row['ref']
            # Find all words joined with a dot
            joined_words = re.findall(r'\b\w+(\.\w+)+\b', ref_value)
            if joined_words:
                # Remove the joined words from the 'ref' column
                for word in joined_words:
                    ref_value = ref_value.replace(word, '')
                # Strip any leading/trailing whitespace or commas
                ref_value = ref_value.strip(', ')
                df.at[index, 'ref'] = ref_value

                # Create the new line to be added
                removed_words = ', '.join(joined_words)
                new_line = f',,,,,,comment,ref,,"update(ref: ""identifier.code"")",4,,,,https://github.com/atviriduomenys/spinta/issues/981,,,'

                # Append the new line to the DataFrame
                new_row = pd.DataFrame([new_line.split(',')], columns=df.columns)
                df = pd.concat([df, new_row], ignore_index=True)

        # Save the modified DataFrame back to the CSV file
        df.to_csv(file_path, index=False)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
    else:
        directory = sys.argv[1]
        process_csv_files(directory)

