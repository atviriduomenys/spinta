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
        if 'prepare' not in row:
            output_lines.append(data_lines[i])
            continue

        # we don't need comments for comments
        if row.get('type') == 'comment':
            output_lines.append(data_lines[i])
            continue

        original_prepare = row['prepare']
        prepare_value = original_prepare
        changed = False  # Track if we made any changes

        # Handle bracket contents more carefully
        position = 0
        result = ''

        # Find all bracket sections
        for match in re.finditer(r'(\w+)(\[.*?\])', prepare_value):
            prefix, brackets = match.groups()
            bracket_content = brackets[1:-1]  # Remove the [ and ]

            # Add text before this match
            result += prepare_value[position:match.start()]

            # Check if there are any dot-separated words in the bracket content
            if re.search(r'\b[\w]+(?:\.[\w]+)+\b', bracket_content):
                # Only process if there are dot-separated words
                parts = [part.strip() for part in bracket_content.split(',')]
                filtered_parts = [part for part in parts if not re.search(r'\b[\w]+(?:\.[\w]+)+\b', part)]
                new_bracket_content = ', '.join(filtered_parts)

                # If we removed something, mark as changed
                if new_bracket_content != bracket_content:
                    changed = True

                # Add the prefix and modified brackets
                result += prefix + '[' + new_bracket_content + ']'
            else:
                # No dot-separated words, keep as is
                result += prefix + brackets

            position = match.end()

        # Add any remaining text
        result += prepare_value[position:]

        # Now process text outside brackets for dot-separated words
        sections = []
        last_end = 0

        for match in re.finditer(r'\[.*?\]', result):
            start, end = match.span()
            # Process text before bracket
            if start > last_end:
                outside_text = result[last_end:start]
                # Check if there are dot-separated words
                if re.search(r'\b[\w]+(?:\.[\w]+)+\b', outside_text):
                    processed_text = remove_dot_words_keep_commas(outside_text)
                    sections.append(processed_text)
                    # If we removed something, mark as changed
                    if processed_text != outside_text:
                        changed = True
                else:
                    sections.append(outside_text)

            # Keep the bracket as is
            sections.append(result[start:end])
            last_end = end

        # Process any remaining text
        if last_end < len(result):
            outside_text = result[last_end:]
            # Check if there are dot-separated words
            if re.search(r'\b[\w]+(?:\.[\w]+)+\b', outside_text):
                processed_text = remove_dot_words_keep_commas(outside_text)
                sections.append(processed_text)
                # If we removed something, mark as changed
                if processed_text != outside_text:
                    changed = True
            else:
                sections.append(outside_text)

        # Join all sections
        prepare_value = ''.join(sections)

        # Clean up spacing and comma formatting if we made changes
        if changed:
            prepare_value = re.sub(r'\s{2,}', ' ', prepare_value)
            prepare_value = re.sub(r',\s*,', ',', prepare_value)
            prepare_value = prepare_value.strip(', ')

        # If no change or no dot words were found, keep original line
        if not changed or prepare_value == original_prepare:
            output_lines.append(data_lines[i])
            continue

        # Replace the old prepare with the cleaned one inside the original line
        updated_line = data_lines[i].replace(original_prepare, prepare_value)
        output_lines.append(updated_line)

        # Add the comment row (quoting minimally to blend with original style)
        comment_row = {
            'type': 'comment',
            'ref': 'ref',
            'prepare': f'update(prepare: "{original_prepare}")',
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


def remove_dot_words_keep_commas(text):
    # Split by commas
    parts = [part.strip() for part in text.split(',')]

    # Filter out parts that are dot-separated words
    filtered_parts = [part for part in parts if not re.search(r'\b[\w]+(?:\.[\w]+)+\b', part)]

    # Rejoin with commas
    return ', '.join(filtered_parts)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
        sys.exit(1)
    else:
        directory = sys.argv[1]
        process_csv_files(directory)
