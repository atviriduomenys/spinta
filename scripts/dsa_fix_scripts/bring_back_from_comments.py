import argparse
import csv
import io
import re
import sys
from pathlib import Path


def process_csv_file(file_path, issue_number, output_dir=None, debug=False):
    # Read original file
    with open(file_path, 'r', encoding='utf-8') as f:
        original_content = f.read()

    if debug:
        print(f"\n===== Processing file: {file_path} =====")

    # Parse with csv.DictReader
    reader = csv.DictReader(io.StringIO(original_content))
    fieldnames = reader.fieldnames
    rows = list(reader)

    if not rows:
        if debug:
            print("File is empty, skipping")
        return  # Skip empty files

    modified_rows = []
    i = 0
    changes_made = False

    if debug:
        print(f"Total rows in file: {len(rows)}")
        print(f"Looking for issue URL: https://github.com/atviriduomenys/spinta/issues/{issue_number}")

    # The exact URL pattern to match
    exact_issue_url = f"https://github.com/atviriduomenys/spinta/issues/{issue_number}"

    while i < len(rows):
        row = rows[i]

        # Check if this is a comment with instructions
        if row.get('type') == 'comment' and row.get('prepare'):
            prepare_text = row.get('prepare', '')
            uri_text = row.get('uri', '')  # Get the uri column
            comment_level = row.get('level', '')  # Get the level from comment

            if debug:
                print(f"\nFound comment row {i}:")
                print(f"  URI: {uri_text}")
                print(f"  Prepare: {prepare_text}")
                print(f"  Level: {comment_level}")

            # Check if the uri column contains the exact issue URL
            issue_match = False
            if exact_issue_url == uri_text:
                issue_match = True
                if debug:
                    print(f"  ✓ Exact match for issue URL in URI")
            elif debug:
                print(f"  ✗ No exact match for issue URL in URI")

            if issue_match:
                # Find the target row (non-comment row before this comment)
                target_idx = -1
                for j in range(len(modified_rows) - 1, -1, -1):
                    if modified_rows[j].get('type') != 'comment':
                        target_idx = j
                        break

                if target_idx >= 0:  # Found a valid target
                    if debug:
                        print(f"  Found target row at index {target_idx} in modified rows")

                    # Check for update action
                    if 'update(' in prepare_text:
                        if debug:
                            print("  Detected update action")
                        update_match = re.search(r'update\((.*?)\)', prepare_text)
                        if update_match:
                            update_params = update_match.group(1)
                            param_pairs = re.findall(r'(\w+): "(.*?)"', update_params)

                            if debug:
                                print(f"  Update parameters: {param_pairs}")

                            # Update the target row
                            for field, value in param_pairs:
                                old_value = modified_rows[target_idx].get(field, '')
                                modified_rows[target_idx][field] = value
                                changes_made = True
                                if debug:
                                    print(f"  Updated field '{field}': '{old_value}' -> '{value}'")

                            # Also update the level from the comment row
                            if comment_level:
                                old_level = modified_rows[target_idx].get('level', '')
                                modified_rows[target_idx]['level'] = comment_level
                                changes_made = True
                                if debug:
                                    print(f"  Updated level: '{old_level}' -> '{comment_level}'")

                    # Check for delete action
                    elif 'delete()' in prepare_text:
                        if debug:
                            print("  Detected delete action")
                            print(f"  Deleting target row: {modified_rows[target_idx]}")
                        # Remove the target row
                        modified_rows.pop(target_idx)
                        changes_made = True
                else:
                    if debug:
                        print("  No valid target row found for this comment")

                # Remove the comment row for the matched issue (don't add it to modified_rows)
                if debug:
                    print(f"  Removing comment row for issue {issue_number}")
                changes_made = True
            else:
                # Keep comments that don't match our issue
                modified_rows.append(row)

            i += 1
            continue

        # Add the current row to output
        modified_rows.append(row)
        i += 1

    # Only write to file if changes were made
    if changes_made:
        # Determine output file path
        original_path = Path(file_path)
        if output_dir:
            output_path = Path(output_dir) / original_path.name
            # Create output directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Default to modifying the original file
            output_path = original_path

        # Write to output file
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(modified_rows)

        print(f"✓ Updated {file_path} (changes applied for issue {issue_number})")
    else:
        print(f"No changes made to {file_path} (no matching issue found or no actions needed)")


def main():
    parser = argparse.ArgumentParser(description='Process comments with instructions in CSV files')
    parser.add_argument('path', help='Path to a CSV file or directory containing CSV files')
    parser.add_argument('--issue', '-i', required=True, help='Issue number to process')
    parser.add_argument('--output-dir', '-o',
                        help='Output directory for processed files (if not specified, original files will be updated)')
    parser.add_argument('--debug', '-d', action='store_true', help='Enable debug output')
    args = parser.parse_args()

    path = Path(args.path)
    issue_number = args.issue
    debug = args.debug

    if debug:
        print(f"Debug mode enabled")
        print(f"Issue number: {issue_number}")
        print(f"Path: {path}")

    if path.is_file() and path.suffix.lower() == '.csv':
        process_csv_file(path, issue_number, args.output_dir, debug)
    elif path.is_dir():
        for csv_file in path.glob('**/*.csv'):
            process_csv_file(csv_file, issue_number, args.output_dir, debug)
    else:
        print(f"Error: {path} is not a CSV file or directory")
        sys.exit(1)


if __name__ == '__main__':
    main()
