import io
import os
import csv
import re
import sys

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
        if 'ref' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row.get('type') == 'comment':
            output_lines.append(data_lines[i])
            continue

        original_ref = row['ref']
        ref_value = original_ref

        # First, handle words inside brackets
        # Find all bracket content
        bracket_contents = re.findall(r'\[(.*?)\]', ref_value)
        
        for content in bracket_contents:
            # Find words with dots (including multiple dots)
            dot_words = re.findall(r'\b[\w]+(?:\.[\w]+)+\b', content)
            
            if dot_words:
                # Create a new content by removing the dot words
                new_content = content
                for word in dot_words:
                    new_content = re.sub(r'(?:,\s*)?' + re.escape(word) + r'(?:\s*,)?', '', new_content)
                
                # Clean up the new content
                new_content = re.sub(r'\s{2,}', ' ', new_content).strip(', ')
                
                # Replace the original bracket content with the cleaned version
                ref_value = ref_value.replace(f'[{content}]', f'[{new_content}]' if new_content else '')
        
        # If there are empty brackets after processing, remove them
        ref_value = re.sub(r'\[\s*\]', '', ref_value)
        
        # Now handle dot-separated words outside brackets
        outside_brackets = re.sub(r'\[.*?\]', '', ref_value)
        dot_words_outside = re.findall(r'\b[\w]+(?:\.[\w]+)+\b', outside_brackets)
        
        for word in dot_words_outside:
            ref_value = re.sub(r'(?:,\s*)?' + re.escape(word) + r'(?:\s*,)?', '', ref_value)
        
        # Clean up spacing and commas
        ref_value = re.sub(r'\s{2,}', ' ', ref_value).strip(', ')
        
        # Fix any remaining empty brackets
        ref_value = re.sub(r'\[\s*\]', '', ref_value)
        
        # Final cleanup for any comma issues
        ref_value = re.sub(r',\s*,', ',', ref_value)
        ref_value = ref_value.strip(', ')
        
        # If no change, keep original line
        if ref_value == original_ref:
            output_lines.append(data_lines[i])
            continue

        # Replace the old ref with the cleaned one inside the original line
        updated_line = data_lines[i].replace(original_ref, ref_value)
        output_lines.append(updated_line)

        # Add the comment row (quoting minimally to blend with original style)
        comment_row = {
            'type': 'comment',
            'ref': 'ref',
            'prepare': f'update(ref: "{original_ref}")',
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
        sys.exit(1)
    else:
        directory = sys.argv[1]
        process_csv_files(directory)
