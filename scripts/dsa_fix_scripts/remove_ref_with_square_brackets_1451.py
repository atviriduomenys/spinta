import argparse
import csv
import io
import os
import re
import sys
from pathlib import Path


def remove_empty_bracket_words(text):
    """
    Remove words containing empty square brackets '[]' from the text.
    For example, "word1, word2[], word3" -> "word1, word3"
    But keeps "word1, word2[content], word3" as is.
    """
    # First, split by commas and process each part separately
    parts = [part.strip() for part in text.split(',')]
    filtered_parts = []
    removed_parts = False
    
    for part in parts:
        # Check if this part contains empty square brackets '[]'
        if '[]' in part:
            print(f"Found empty brackets in: '{part}'")
            removed_parts = True
            continue  # Skip parts with empty square brackets
            
        # If we got here, the part has no empty square brackets, so keep it
        filtered_parts.append(part)
    
    clean_result = ', '.join(filtered_parts)
    
    # Clean up extra spaces and commas
    clean_result = re.sub(r'\s{2,}', ' ', clean_result)
    clean_result = re.sub(r',\s*,', ',', clean_result)
    clean_result = clean_result.strip(', ')
    
    if removed_parts:
        print(f"Original: '{text}'")
        print(f"Cleaned: '{clean_result}'")
    
    return clean_result, removed_parts


def process_csv_file(file_path):
    print(f"\nProcessing file: {file_path}")
    
    # Read original file lines as text
    with open(file_path, 'r', encoding='utf-8') as f:
        original_lines = f.readlines()

    if not original_lines:
        return  # Skip empty files

    # Parse with csv.DictReader using StringIO
    reader = csv.DictReader(io.StringIO(''.join(original_lines)))
    fieldnames = reader.fieldnames
    rows = list(reader)

    header_line = original_lines[0]
    data_lines = original_lines[1:]

    output_lines = [header_line]  # Start with header
    changes_made = 0

    for i, row in enumerate(rows):
        if 'ref' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row.get('type') == 'comment':
            output_lines.append(data_lines[i])
            continue

        original_ref = row['ref']
        original_level = row['level']

        # Use the improved function for cleaning
        ref_value, brackets_removed = remove_empty_bracket_words(original_ref)

        # If no change was made or no brackets were removed, keep original line
        if ref_value == original_ref or not brackets_removed:
            output_lines.append(data_lines[i])
            continue

        changes_made += 1
        print(f"Change #{changes_made} - Removing empty brackets from ref value")
        
        # Replace the old ref with the cleaned one inside the original line
        row['level'] = 1
        row['ref'] = ref_value
        # Write the modified row using csv to match format
        with io.StringIO() as buf:
            writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(row)
            modified_line = buf.getvalue()
        output_lines.append(modified_line)

        # Add the comment row (quoting minimally to blend with original style)
        comment_row = {
            'type': 'comment',
            'ref': 'ref',
            'prepare': f'update(ref: "{original_ref}")',
            'level': original_level,
            'visibility': 'protected',
            'uri': 'https://github.com/atviriduomenys/spinta/issues/1451'
        }

        # Write comment row using csv to match format
        with io.StringIO() as buf:
            writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(comment_row)
            comment_line = buf.getvalue()
        output_lines.append(comment_line)

    if changes_made == 0:
        print("No empty brackets found in this file.")
    else:
        print(f"Made {changes_made} changes in the file.")

    # Write back to file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(output_lines)


def main():
    parser = argparse.ArgumentParser(description='Remove references containing empty square brackets from CSV files')
    parser.add_argument('path', help='Path to a CSV file or directory containing CSV files')
    args = parser.parse_args()

    path = Path(args.path)

    if path.is_file() and path.suffix.lower() == '.csv':
        process_csv_file(path)
    elif path.is_dir():
        for csv_file in path.glob('**/*.csv'):
            process_csv_file(csv_file)
    else:
        print(f"Error: {path} is not a CSV file or directory")
        sys.exit(1)


if __name__ == '__main__':
    main()
