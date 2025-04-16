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
        reader = csv.DictReader(file)
        # Normalize fieldnames by stripping whitespace
        original_fieldnames = reader.fieldnames
        normalized_fieldnames = [field.strip() for field in original_fieldnames]
        rows = list(reader)

    # Write the modified rows back to the CSV file with normalized column names
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=normalized_fieldnames)
        writer.writeheader()
        for row in rows:
            # Write each row with normalized column names
            writer.writerow({normalized: row[original] for original, normalized in zip(original_fieldnames, normalized_fieldnames)})

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python normalize_columns.py <directory>")
    else:
        directory = sys.argv[1]
        normalize_column_names(directory)
