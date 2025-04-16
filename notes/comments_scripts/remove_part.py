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
                        word_with_part = next(part for part in parts if '/:part' in part).strip('"')

                        new_line = line.replace('/:part', '')
                        file.write(new_line)

                        file.write(f',,,,,,comment,model,,"update(model: ""{word_with_part}"")",4,,,,https://github.com/atviriduomenys/spinta/issues/997,,,\n')
                    else:
                        file.write(line)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
    else:
        directory = sys.argv[1]
        process_files_recursively(directory)

