#%%
#!/usr/bin/env python3
"""
Model Values Counter Script

This script traverses through a nested directory structure,
finds all CSV files, and counts the total number of values
in the 'model' column across all files.
"""

import os
import pandas as pd
import glob
import argparse
import sys

def count_model_values(root_dir):
    """
    Count the total number of values in the 'model' column across all CSV files

    Args:
        root_dir (str): The root directory to start searching from

    Returns:
        int: Total count of values in the 'model' column
    """
    # Initialize counter
    total_count = 0

    # Keep track of processed files
    processed_files = 0
    files_with_model = 0

    print(f"Searching for CSV files in {root_dir}...")

    # Find all CSV files recursively
    csv_files = glob.glob(os.path.join(root_dir, '**', '*.csv'), recursive=True)

    if not csv_files:
        print(f"No CSV files found in {root_dir}")
        return total_count

    print(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)

            # Check if 'model' column exists
            if 'model' not in df.columns:
                print(f"Warning: No 'model' column in {csv_file}")
                continue

            # Count non-null values in the model column
            model_count = df['model'].count()
            total_count += model_count

            if model_count > 0:
                files_with_model += 1

            processed_files += 1

        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")

    print(f"Successfully processed {processed_files} CSV files")
    print(f"Found 'model' column in {files_with_model} files")

    return total_count

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Count values in the model column of CSV files.')
    parser.add_argument('directory', help='Root directory containing CSV files')

    args = parser.parse_args()

    # Check if directory exists
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        sys.exit(1)

    # Run the counting function
    total_model_values = count_model_values(args.directory)

    # Print results
    print(f"\nResults:")
    print(f"Total number of values in 'model' column across all CSV files: {total_model_values}")

if __name__ == "__main__":
    main()
