import os
import csv
import io
import sys


def trim_csv_spaces(directory):
    """
    Recursively process all CSV files in a directory, trimming spaces
    before and after values in each field.

    Args:
        directory (str): Path to directory containing CSV files
    """
    processed_count = 0

    # Walk through all files in directory and subdirectories
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                if process_file(file_path):
                    processed_count += 1

    print(f"Processed {processed_count} CSV files")


def process_file(file_path):
    """
    Process a single CSV file, trimming spaces from all field values.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if not content.strip():
            # Skip empty files
            return False

        # Process the file
        original_rows = []
        trimmed_rows = []

        # Use csv reader to parse the content
        reader = csv.reader(io.StringIO(content))
        for row in reader:
            original_rows.append(row)
            # Trim spaces from each field
            trimmed_row = [field.strip() if field else field for field in row]
            trimmed_rows.append(trimmed_row)

        # Skip if no change was made
        if original_rows == trimmed_rows:
            return False

        # Write back to the file
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(trimmed_rows)

        print(f"Trimmed spaces in: {file_path}")
        return True

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python trim_csv_spaces.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory")
        sys.exit(1)

    trim_csv_spaces(directory)
