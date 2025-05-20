import os
import sys

def reverse_process_files_recursively(directory):
    # Walk through the directory tree
    for root, _, files in os.walk(directory):
        for filename in files:
            file_path = os.path.join(root, filename)

            with open(file_path, 'r') as file:
                lines = file.readlines()

            with open(file_path, 'w') as file:
                i = 0
                while i < len(lines):
                    line = lines[i]
                    if ',comment,' in line and '/:part' in line:
                        parts = line.split(',')
                        word_with_part = next(part for part in parts if '/:part' in part)
                        word_before_part = word_with_part.replace('/:part', '').strip('"')

                        previous_line = lines[i - 1]
                        updated_line = ','.join([part if word_before_part not in part else part + '/:part' for part in previous_line.split(',')])

                        file.write(updated_line)
                        i += 1  # Skip the next iteration to remove the comment line
                    else:
                        file.write(line)
                    i += 1

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
    else:
        directory = sys.argv[1]
        reverse_process_files_recursively(directory)
