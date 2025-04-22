import io
import os
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
        if 'property' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row["type"] == 'comment':
            output_lines.append(data_lines[i])
            continue


        original_property = row['property']
        property_value = original_property

        # Match dot-separated fields (including optional [])
        property_to_change = re.findall(r'[\w]+\.[\w]+@[\w]+', property_value)


        if not property_to_change:
            output_lines.append(data_lines[i])
            continue

        property_to_change = property_to_change[0]

        property_value = property_to_change.split('@')[0]

        # Replace the old property with the cleaned one inside the original line
        updated_line = data_lines[i].replace(original_property, property_value)
        output_lines.append(updated_line)

        # Add the comment row (quoting minimally to blend with original style)
        comment_row = {
            'type': 'comment',
            'ref': 'property',
            'prepare': f'update(property: "{original_property}")',
            'level': 4,
            'visibility': 'public',
            'uri': 'https://github.com/atviriduomenys/spinta/issues/1289'
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
    # if len(sys.argv) != 2:
    #     print("Usage: python script.py <directory>")
    # else:
    #     directory = sys.argv[1]
    #     process_csv_files(directory)
    directory = "/home/karina/work/vssa/metadata/datasets/gov/rc"
    process_csv_files(directory)



