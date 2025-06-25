import argparse
import csv
import io
import re
import sys
from pathlib import Path


def process_csv_file(file_path, output_dir=None):
    # Read original file
    with open(file_path, 'r', encoding='utf-8') as f:
        original_content = f.read()

    # Parse with csv.DictReader
    reader = csv.DictReader(io.StringIO(original_content))
    fieldnames = reader.fieldnames
    rows = list(reader)

    if not rows:
        return  # Skip empty files

    modified_rows = []
    i = 0

    while i < len(rows):
        row = rows[i]

        # Check if this is a comment with instructions
        if row.get('type') == 'comment' and row.get('prepare'):
            prepare_text = row.get('prepare', '')

            # Find the target row (non-comment row before this comment)
            target_idx = -1
            for j in range(len(modified_rows) - 1, -1, -1):
                if modified_rows[j].get('type') != 'comment':
                    target_idx = j
                    break

            if target_idx >= 0:  # Found a valid target
                # Check for update action
                if 'update(' in prepare_text:
                    update_match = re.search(r'update\((.*?)\)', prepare_text)
                    if update_match:
                        update_params = update_match.group(1)
                        param_pairs = re.findall(r'(\w+): "(.*?)"', update_params)

                        # Update the target row
                        for field, value in param_pairs:
                            modified_rows[target_idx][field] = value

                # Check for delete action
                elif 'delete()' in prepare_text:
                    # Remove the target row
                    modified_rows.pop(target_idx)

                # Note: Add action would be implemented here if needed

            # Skip this comment row
            i += 1
            continue

        # Add the current row to output
        modified_rows.append(row)
        i += 1

    # Determine output file path
    original_path = Path(file_path)
    if output_dir:
        output_path = Path(output_dir) / original_path.name
    else:
        # Create output in the same directory with modified name
        stem = original_path.stem
        output_path = original_path.with_name(f"{stem}_processed{original_path.suffix}")

    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to output file
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(modified_rows)

    print(f"Processed {file_path} -> {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Process comments with instructions in CSV files')
    parser.add_argument('path', help='Path to a CSV file or directory containing CSV files')
    parser.add_argument('--output-dir', '-o', help='Output directory for processed files')
    args = parser.parse_args()

    path = Path(args.path)

    if path.is_file() and path.suffix.lower() == '.csv':
        process_csv_file(path, args.output_dir)
    elif path.is_dir():
        for csv_file in path.glob('**/*.csv'):
            process_csv_file(csv_file, args.output_dir)
    else:
        print(f"Error: {path} is not a CSV file or directory")
        sys.exit(1)


if __name__ == '__main__':
    main()
