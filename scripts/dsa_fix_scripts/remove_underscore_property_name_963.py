# https://github.com/atviriduomenys/spinta/issues/963

import io
import os
import csv


def process_csv_files(directory):
    # Recursively walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                process_csv_file(file_path)
                print(f"Processed: {file_path}")  # Add debug output

def get_level(property_name):
    # underscored property can be in the middle of the property name
    property_names = property_name.split('.')

    for name in property_names:
        if name.startswith('_'):
            property_name = name
            break
    if property_name.split('@')[0] in ['_id', '_revision', '_created', '_updated', '_label']:
        return 4
    else:
        return 2


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

    # todo maybe add support for prepare (not needed now though)

    for i, row in enumerate(rows):
        if 'property' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row["type"] == 'comment':
            output_lines.append(data_lines[i])
            continue

        if (not row["property"].startswith('_')
            and not "._" in row["property"]
            and not ' _' in row["ref"]
            and not '[_' in row["ref"]
            and not ' _' in row["base"]
            and not '[_' in row["base"]
        ):
            output_lines.append(data_lines[i])
            continue

        old_property = row["property"]
        old_ref = row["ref"]
        old_base = row["base"]
        old_level = row["level"]

        comment_row_property = ''
        comment_row_ref = ''
        comment_row_base = ''
        new_property_level = old_level

        # remove initial underscore
        if row["property"].startswith('_') or '._' in row["property"]:
            new_property_level = get_level(row["property"])
            row["property"] = row["property"].lstrip('_')

            # remove underscore in property name
            row["property"] = row["property"].replace('._', '.')

            # Add the comment row (quoting minimally to blend with original style)
            comment_row_property = {
                'type': 'comment',
                'ref': 'property',
                'prepare': f'update(property: "{old_property}")',
                'visibility': 'protected',
                'uri': 'https://github.com/atviriduomenys/spinta/issues/963',
                'level': old_level,
            }

        if " _" in row["ref"] or '[_' in row["ref"]:
            row["ref"] = row["ref"].replace(' _', ' ')

            # remove underscore in property name
            row["ref"] = row["ref"].replace('[_', '.')

            # Add the comment row (quoting minimally to blend with original style)
            comment_row_ref = {
                'type': 'comment',
                'ref': 'ref',
                'prepare': f'update(ref: "{old_ref}")',
                'visibility': 'protected',
                'uri': 'https://github.com/atviriduomenys/spinta/issues/963',
                'level': old_level,
            }

        if " _" in row["base"] or '[_' in row["base"]:
            row["base"] = row["base"].replace(' _', ' ')

            # remove underscore in property name
            row["base"] = row["base"].replace('[_', '.')

            # Add the comment row (quoting minimally to blend with original style)
            comment_row_base = {
                'type': 'comment',
                'ref': 'base',
                'prepare': f'update(base: "{old_base}")',
                'visibility': 'protected',
                'uri': 'https://github.com/atviriduomenys/spinta/issues/963',
                'level': old_level,
            }

        row["level"] = new_property_level

        # Write the modified row using csv to match format
        with io.StringIO() as buf:
            writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(row)
            modified_line = buf.getvalue()
        output_lines.append(modified_line)

        if comment_row_property:

            # Write comment row using csv to match format
            with io.StringIO() as buf:
                writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(comment_row_property)
                comment_line = buf.getvalue()
            output_lines.append(comment_line)

        if comment_row_ref:

            # Write comment row using csv to match format
            with io.StringIO() as buf:
                writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(comment_row_ref)
                comment_line = buf.getvalue()
            output_lines.append(comment_line)

        if comment_row_base:

            # Write comment row using csv to match format
            with io.StringIO() as buf:
                writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(comment_row_base)
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
    # directory = "/home/karina/work/vssa/metadata/datasets/gov/rc/stsr_ws/n903_hipotekos_duomenu_israsas_is_ntr_ir_stsr_su_pateiktu_dokumentu_kopijomis_pagal_hipotekos_idk"
    process_csv_files(directory)
