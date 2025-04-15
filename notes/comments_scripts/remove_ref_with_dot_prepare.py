import os
import sys
import csv
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
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames
        rows = list(reader)

    modified_rows = []
    for row_number, row in enumerate(rows):
        try:
            prepare_value = row['prepare']
        except KeyError as e:
            raise

        # if it's already a comment, we don't need to remove anything
        if row.get("type") == "comment":
            modified_rows.append(row)
            continue
        # Find all words joined with a dot
        joined_words = re.findall(r'\b[\w\[\]]+(?:\.[\w\[\]]+)+\b', prepare_value)
        original_prepare_value = prepare_value
        if joined_words:
            for word in joined_words:
                pattern = r'(?:,\s*)?' + re.escape(word) + r'(?:\s*,)?'
                prepare_value = re.sub(pattern, '', prepare_value)

            # Clean up any leftover stray commas or extra spaces
            prepare_value = re.sub(r'\s{2,}', ' ', prepare_value)
            prepare_value = prepare_value.strip(', ').strip()

            # Remove empty or fully cleaned-out square brackets (e.g., something[] -> something)
            prepare_value = re.sub(r'(\w+)\[\s*(,?\s*)*\]', r'\1', prepare_value)
            prepare_value = re.sub(r'\[\s*\]', '', prepare_value)
            # Trim spaces inside square brackets (e.g., [ nr , code ] -> [nr, code])
            prepare_value = re.sub(r'\[([^\]]+)\]',
                               lambda m: '[' + ', '.join(part.strip() for part in m.group(1).split(',')) + ']',
                               prepare_value)

            row['prepare'] = prepare_value

            # Create the new line to be added
            new_line = {
                'type': 'comment',
                'ref': 'prepare',
                'prepare': f'update(prepare: "{original_prepare_value}")',
                'level': 4,  # Example numeric column
                'visibility': "public",
                'uri': 'https://github.com/atviriduomenys/spinta/issues/981'
            }
            # Add the modified row
            modified_rows.append(row)
            # Add the new comment line immediately after the modified row
            modified_rows.append(new_line)
        else:
            # Add the unmodified row
            modified_rows.append(row)

    # Write the modified rows back to the CSV file
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(modified_rows)

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: python script.py <directory>")
    # else:
    #     directory = sys.argv[1]
    #     process_csv_files(directory)
    directory = "/home/karina/work/vssa/metadata/datasets/gov/rc"
    process_csv_files(directory)



