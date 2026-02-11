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
                apply_ref_updates_in_csv(file_path)


def apply_ref_updates_in_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    reader = csv.DictReader(lines)
    fieldnames = reader.fieldnames
    rows = list(reader)

    updated_rows = []
    number_changed_rows = 0
    i = 0
    while i < len(rows):
        row = rows[i]

        if row.get('type') == 'comment' and "@" in row.get('prepare', ''):
            # Extract the update(ref: "value") content
            match = re.search(r'update\(ref:\s*"([^"]+)"\)', row['prepare'])
            if match:
                new_ref = match.group(1)
                if i > 0:
                    prev_row = rows[i - 1]
                    old_ref = prev_row.get('ref', '')

                    # If new_ref is like "prefix@suffix"
                    if '@' in new_ref:
                        prefix = new_ref.split('@')[0]
                        parts = [part.strip() for part in old_ref.split(',') if part.strip()]
                        replaced = False
                        for j, part in enumerate(parts):
                            if part == prefix:
                                parts[j] = new_ref
                                replaced = True
                        if not replaced:
                            parts.append(new_ref)
                        prev_row['ref'] = ', '.join(parts)
                        updated_rows[-1] = prev_row  # Update last written row
                        number_changed_rows += 1

            # Skip the comment row (i.e., do not add it to updated_rows)
            i += 1
            continue

        updated_rows.append(row)
        i += 1

    if number_changed_rows:
    # Write back to file
        with open(file_path, 'w', encoding='utf-8', newline='') as f:

            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(updated_rows)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
    else:
        directory = sys.argv[1]
        process_csv_files(directory)

# patikrinti, ar viskas ok su `level`
