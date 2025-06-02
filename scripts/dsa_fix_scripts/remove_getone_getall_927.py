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

    model_with_functions_counts = {}

    for i, row in enumerate(rows):

        if 'type' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row["type"] == 'comment':
            output_lines.append(data_lines[i])
            continue

        if (not "/:getone" in row["model"]) and (not "/:getall" in row["model"]):
            output_lines.append(data_lines[i])
            continue
        function = row["model"].split('/:')[1].split('?')[0]
        model_name = row["model"].split('/:')[0]
        # Change type from money to string
        old_model = row["model"]

        model_number = ""
        if model_name not in model_with_functions_counts:
            model_with_functions_counts[model_name] = 0
        else:
            model_with_functions_counts[model_name] += 1
            if model_with_functions_counts[model_name] > 0:
                model_number = str(model_with_functions_counts[model_name])
        row["model"] = row["model"].replace(f'/:{function}', model_number)


        # Write the modified row using csv to match format
        with io.StringIO() as buf:
            writer = csv.DictWriter(buf, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(row)
            modified_line = buf.getvalue()
        output_lines.append(modified_line)

        # Add the comment row (quoting minimally to blend with original style)
        comment_row = {
            'type': 'comment',
            'ref': 'model',
            'prepare': f'update(model: "{old_model}")',
            'visibility': 'public',
            'uri': 'https://github.com/atviriduomenys/spinta/issues/927'
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
    # directory = "/home/karina/work/vssa/metadata/datasets/gov/rc/stsr_ws/n903_hipotekos_duomenu_israsas_is_ntr_ir_stsr_su_pateiktu_dokumentu_kopijomis_pagal_hipotekos_idk"
    process_csv_files(directory)
