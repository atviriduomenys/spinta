import io
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
        if 'prepare' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row["type"] == 'comment':
            output_lines.append(data_lines[i])
            continue

        original_prepare = row['prepare']
        prepare_value = original_prepare

        print(f"Type of prepare_value: {type(prepare_value)}")
        print(f"Value of prepare_value: {prepare_value}")

        # Make sure prepare_value is a string
        if prepare_value is None:
            prepare_value = ''
        elif not isinstance(prepare_value, str):
            prepare_value = str(prepare_value)

        # Match dot-separated fields (including optional [])
        joined_words = re.findall(r'\b[\w\[\]]+(?:\.[\w\[\]]+)+\b', prepare_value)

        if not joined_words:
            output_lines.append(data_lines[i])
            continue

        # Remove each matched word + surrounding commas/whitespace
        for word in joined_words:
            pattern = r'(?:,\s*)?' + re.escape(word) + r'(?:\s*,)?'
            prepare_value = re.sub(pattern, '', prepare_value)

        # Clean spacing
        prepare_value = re.sub(r'\s{2,}', ' ', prepare_value).strip(', ').strip()

        # Remove empty brackets
        prepare_value = re.sub(r'(\w+)\[\s*(,?\s*)*\]', r'\1', prepare_value)
        prepare_value = re.sub(r'\[\s*\]', '', prepare_value)

        # Clean spacing inside brackets
        prepare_value = re.sub(
            r'\[([^\]]+)\]',
            lambda m: '[' + ', '.join(part.strip() for part in m.group(1).split(',')) + ']',
            prepare_value
        )

        # If no change, keep original line
        if prepare_value == original_prepare:
            output_lines.append(data_lines[i])
            continue

        # Replace the old prepare with the cleaned one inside the original line
        updated_line = data_lines[i].replace(original_prepare, prepare_value)
        output_lines.append(updated_line)

        # Add the comment row (quoting minimally to blend with original style)
        comment_row = {
            'type': 'comment',
            'ref': 'prepare',
            'prepare': f'update(prepare: "{original_prepare}")',
            'level': 4,
            'visibility': 'public',
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


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
    else:
        directory = sys.argv[1]
        process_csv_files(directory)
