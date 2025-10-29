#%%
#!/usr/bin/env python3
"""
Question Mark Model Counter Script

This script traverses through a nested directory structure,
finds all CSV files, and counts how many values in the 'model' column
contain a question mark "?" character.
"""

import os
import pandas as pd
import glob
import argparse
import sys

def count_question_mark_models(root_dir):
    """
    Count the number of values in the 'model' column that contain a question mark

    Args:
        root_dir (str): The root directory to start searching from

    Returns:
        tuple: (total_question_mark_count, list of question mark models with counts)
    """
    # Initialize counters
    total_question_mark_count = 0
    question_mark_models = {}  # To track specific models containing question marks

    # Keep track of processed files
    processed_files = 0
    files_with_model = 0

    print(f"Searching for CSV files in {root_dir}...")

    # Find all CSV files recursively
    csv_files = glob.glob(os.path.join(root_dir, '**', '*.csv'), recursive=True)

    if not csv_files:
        print(f"No CSV files found in {root_dir}")
        return total_question_mark_count, question_mark_models

    print(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)

            # Check if 'model' column exists
            if 'model' not in df.columns:
                print(f"Warning: No 'model' column in {csv_file}")
                continue

            # Get models with question marks
            question_mark_df = df[df['model'].fillna('').astype(str).str.contains(r'\?')]

            # Count question mark models
            file_q_mark_count = len(question_mark_df)
            total_question_mark_count += file_q_mark_count

            # Track specific models with question marks
            if file_q_mark_count > 0:
                model_counts = question_mark_df['model'].value_counts().to_dict()
                for model, count in model_counts.items():
                    if model in question_mark_models:
                        question_mark_models[model] += count
                    else:
                        question_mark_models[model] = count

            files_with_model += 1
            processed_files += 1

        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")

    print(f"Successfully processed {processed_files} CSV files")
    print(f"Found 'model' column in {files_with_model} files")

    return total_question_mark_count, question_mark_models

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Count models with question marks in CSV files.')
    parser.add_argument('directory', help='Root directory containing CSV files')
    parser.add_argument('-t', '--top', type=int, default=10,
                      help='Number of top question mark models to display (default: 10)')
    parser.add_argument('-o', '--output', help='Output file for question mark models (optional)')

    args = parser.parse_args()

    # Check if directory exists
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        sys.exit(1)

    # Run the counting function
    total_count, q_mark_models = count_question_mark_models(args.directory)

    # Print results
    print(f"\nResults:")
    print(f"Total number of models with question marks: {total_count}")
    print(f"Number of unique models with question marks: {len(q_mark_models)}")

    if q_mark_models:
        print(f"\nTop {min(args.top, len(q_mark_models))} models with question marks:")
        sorted_models = sorted(q_mark_models.items(), key=lambda x: x[1], reverse=True)

        for i, (model, count) in enumerate(sorted_models[:args.top]):
            print(f"{i+1}. '{model}' - {count} occurrences")

        # Save to file if requested
        if args.output:
            try:
                df = pd.DataFrame(sorted_models, columns=['Model', 'Count'])
                df.to_csv(args.output, index=False)
                print(f"\nSaved all {len(q_mark_models)} question mark models to {args.output}")
            except Exception as e:
                print(f"Error saving to file: {str(e)}")

if __name__ == "__main__":
    main()
