import os
import sys
import csv
import re
import io

def process_csv_files(directory):
    # Recursively walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                process_csv_file(file_path)

def process_csv_file(file_path):
    try:
        # First, read the file line by line to preserve original formatting
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            lines = file.readlines()
        
        if not lines:
            print(f"Empty file: {file_path}")
            return
            
        # Parse the header to get column names
        reader = csv.reader([lines[0].strip()])
        header = next(reader)
        
        # Identify the indexes of important columns
        type_idx = header.index('type') if 'type' in header else None
        prepare_idx = header.index('prepare') if 'prepare' in header else None
        uri_idx = header.index('uri') if 'uri' in header else None
        ref_idx = header.index('ref') if 'ref' in header else None
        source_idx = header.index('source') if 'source' in header else None
        
        if type_idx is None:
            print(f"No 'type' column found in {file_path}, skipping.")
            return
        
        # Process data row by row
        modified_lines = [lines[0]]  # Keep header as is
        modified = False
        
        for i in range(1, len(lines)):
            line = lines[i]
            stripped_line = line.strip()
            if not stripped_line:  # Skip empty lines
                modified_lines.append(line)
                continue
                
            # Parse the row
            reader = csv.reader([stripped_line])
            row_values = next(reader)
            
            if len(row_values) <= type_idx or row_values[type_idx].strip().lower() != 'comment':
                # Not a comment row, leave it unchanged
                modified_lines.append(line)
                continue
            
            # Check if any cell contains our pattern
            pattern_found = False
            pattern_cell_content = None
            
            for cell in row_values:
                if 'add(type:"enum"' in cell:
                    pattern_found = True
                    pattern_cell_content = cell
                    break
            
            if not pattern_found:
                # No pattern in this comment row, leave it unchanged
                modified_lines.append(line)
                continue
            
            # We found a row to modify - make a copy so we don't change the original
            new_row_values = row_values.copy()
            
            # Change type to enum
            new_row_values[type_idx] = 'enum'
            
            # Extract and update prepare value if it exists
            if prepare_idx is not None:
                prepare_match = re.search(r"prepare:\s*'([^']*)'", pattern_cell_content)
                if prepare_match:
                    new_row_values[prepare_idx] = f"'{prepare_match.group(1)}'"
            
            # Handle uri field
            if uri_idx is not None:
                # Clear existing uri value
                new_row_values[uri_idx] = ''
                
                # If there's a uri in the "add" section, use that value
                uri_match = re.search(r'uri:\s*"([^"]*)"', pattern_cell_content)
                if uri_match:
                    new_row_values[uri_idx] = uri_match.group(1)
            
            # Clear ref and source if they exist
            if ref_idx is not None:
                new_row_values[ref_idx] = ''
            if source_idx is not None:
                new_row_values[source_idx] = ''
            
            # Write the modified row back
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(new_row_values)
            new_line = output.getvalue()
            
            # Handle line endings correctly - keep the original line ending
            if line.endswith('\r\n'):
                if not new_line.endswith('\r\n'):
                    new_line = new_line.rstrip('\n') + '\r\n'
            elif line.endswith('\n'):
                if not new_line.endswith('\n'):
                    new_line = new_line.rstrip('\n') + '\n'
            
            modified_lines.append(new_line)
            modified = True
        
        # Only write back to the file if we made changes
        if modified:
            with open(file_path, mode='w', newline='', encoding='utf-8') as file:
                file.writelines(modified_lines)
                
            print(f"Successfully processed: {file_path}")
        else:
            print(f"No matching patterns found in: {file_path}")
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python bring_back_enum_nested.py <directory>")
    else:
        directory = sys.argv[1]
        process_csv_files(directory)
