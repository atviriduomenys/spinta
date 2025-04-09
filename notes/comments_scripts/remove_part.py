import os
import sys

def process_files_recursively(directory):
    # Walk through the directory tree
    for root, _, files in os.walk(directory):
        for filename in files:
            file_path = os.path.join(root, filename)

            with open(file_path, 'r') as file:
                lines = file.readlines()

            with open(file_path, 'w') as file:
                for line in lines:
                    if '/:part' in line:
                        parts = line.split(',')
                        word_with_part = next(part for part in parts if '/:part' in part)
                        word_before_part = word_with_part.replace('/:part', '')

                        new_line = line.replace('/:part', '')
                        file.write(new_line)

                        file.write(f',,,,comment,{word_before_part},,"update(name: \"{word_before_part}/:part\")",4,,,,https://github.com/atviriduomenys/spinta/issues/997,,,\n')
                    else:
                        file.write(line)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
    else:
        directory = sys.argv[1]
        process_files_recursively(directory)

