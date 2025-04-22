import os
import sys
import csv

def normalize_column_names(directory):
    # Recursively walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                normalize_column_names_in_file(file_path)

def normalize_column_names_in_file(file_path):
    # Read the CSV file
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        # Read the first line (header) and the rest of the file
        header = file.readline().strip()
        content = file.read()
    
    # Normalize header by stripping whitespace from each field
    normalized_header = ','.join([field.strip() for field in header.split(',')])
    
    # Write the normalized header and the original content back to the file
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        file.write(normalized_header + '\n' + content)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python normalize_columns.py <directory>")
    else:
        directory = sys.argv[1]
        normalize_column_names(directory)
