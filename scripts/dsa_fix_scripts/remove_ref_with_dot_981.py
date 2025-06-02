import argparse
import csv
import io
import os
import re
import sys
from pathlib import Path


def remove_dot_words(text):
    """
    Remove words containing dots from the text.
    For example, "word1, word2.field1, word3" -> "word1, word3"
    Also handles brackets properly, removing empty brackets or keeping with content.
    """
    # Handle the bracket content specifically
    def process_bracket_content(match):
        prefix = match.group(1)  # Everything before the opening bracket
        content = match.group(2)  # Content inside brackets
        
        # Split by comma and filter out dot-containing parts
        parts = [part.strip() for part in content.split(',')]
        filtered_parts = [part for part in parts if '.' not in part]
        
        if not filtered_parts:
            # If all parts were removed, return just the prefix without brackets
            return prefix
        else:
            # Otherwise, rebuild with the remaining content
            return f"{prefix}[{', '.join(filtered_parts)}]"
    
    # Find paths with brackets and process their content
    bracket_pattern = r'(.*?)\[(.*?)\]'
    result = re.sub(bracket_pattern, process_bracket_content, text)
    
    # Then handle standard dot-separated words outside of brackets
    parts = [part.strip() for part in result.split(',')]
    filtered_parts = [part for part in parts if not re.search(r'\b[\w]+(?:\.[\w]+)+\b', part)]
    clean_result = ', '.join(filtered_parts)
    
    # Clean up extra spaces and commas
    clean_result = re.sub(r'\s{2,}', ' ', clean_result)
    clean_result = re.sub(r',\s*,', ',', clean_result)
    clean_result = clean_result.strip(', ')
    
    return clean_result


def process_csv_file(file_path):
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
        ref_value = remove_dot_words(original_ref)

        # If no change was made, keep original line
        if ref_value == original_ref:
            output_lines.append(data_lines[i])
            continue

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
            'uri': 'https://github.com/atviriduomenys/spinta/issues/981'
        }

        # Write comment row using csv to match format
        with io.StringIO() as buf:
            writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(comment_row)
            comment_line = buf.getvalue()
        output_lines.append(comment_line)

    # Write back to file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(output_lines)


def main():
    parser = argparse.ArgumentParser(description='Remove references containing dots from CSV files')
    parser.add_argument('path', help='Path to a CSV file or directory containing CSV files')
    args = parser.parse_args()

    path = Path(args.path)

    if path.is_file() and path.suffix.lower() == '.csv':
        print(f"Processing file: {path}")
        process_csv_file(path)
    elif path.is_dir():
        for csv_file in path.glob('**/*.csv'):
            print(f"Processing file: {csv_file}")
            process_csv_file(csv_file)
    else:
        print(f"Error: {path} is not a CSV file or directory")
        sys.exit(1)


if __name__ == '__main__':
    main()
