import os
import sys
import csv
import io

def normalize_column_names(directory):
    # Recursively walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                normalize_column_names_in_file(file_path)

def normalize_column_names_in_file(file_path):
    try:
        # Read the CSV file
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            # Read just the header line to get column names
            reader = csv.reader(file)
            header = next(reader)
            
            # Read the rest of the content
            content = file.read()
        
        # Normalize header: remove spaces and strip quotation marks
        normalized_header = []
        for field in header:
            # Remove spaces and quotation marks
            normalized_field = field.strip()
            # Remove quotation marks if they exist
            if normalized_field.startswith('"') and normalized_field.endswith('"'):
                normalized_field = normalized_field[1:-1].strip()
            normalized_header.append(normalized_field)
        
        # Create a new CSV header line
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(normalized_header)
        normalized_header_line = output.getvalue().strip()
        
        # Write the normalized header and the original content back to the file
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            file.write(normalized_header_line + '\n' + content)
            
        print(f"Successfully normalized headers in: {file_path}")
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python normalize_columns.py <directory>")
    else:
        directory = sys.argv[1]
        normalize_column_names(directory)
