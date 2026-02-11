#%%
#!/usr/bin/env python3
"""
URI Analysis Script for CSV Files

This script analyzes CSV files in a nested directory structure,
extracts values from the 'uri' column, and calculates:
- Total frequencies of each URI value
- In how many files each URI value exists
"""

import os
import pandas as pd
from collections import Counter
import glob
import argparse
import sys

def analyze_csv_files(root_dir):
    """
    Analyze CSV files in a directory structure to find frequencies of uri values

    Args:
        root_dir (str): The root directory to start searching from

    Returns:
        tuple: (total_frequency_counter, file_occurrence_counter)
    """
    # Counter for total occurrences of each URI
    total_frequency = Counter()

    # Counter for number of files each URI appears in
    file_occurrence = Counter()

    # Keep track of processed files
    processed_files = 0

    print(f"Searching for CSV files in {root_dir}...")

    # Find all CSV files recursively
    csv_files = glob.glob(os.path.join(root_dir, '**', '*.csv'), recursive=True)

    if not csv_files:
        print(f"No CSV files found in {root_dir}")
        return total_frequency, file_occurrence

    print(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)

            # Check if 'uri' column exists
            if 'uri' not in df.columns:
                print(f"Warning: No 'uri' column in {csv_file}")
                continue

            # Extract URI values
            uris = df['uri'].dropna().tolist()

            # Count occurrences in this file
            file_counter = Counter(uris)

            # Update total frequency counter
            total_frequency.update(file_counter)

            # Update file occurrence counter (count 1 for each unique uri in this file)
            file_occurrence.update(set(uris))

            processed_files += 1

        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")

    print(f"Successfully processed {processed_files} CSV files")
    return total_frequency, file_occurrence

def print_results(total_freq, file_occur, top_n=20):
    """Print the analysis results"""
    if not total_freq:
        print("No URI data found in the processed files.")
        return

    print(f"\n=== Top {top_n} Most Frequent URIs ===")
    for uri, count in total_freq.most_common(top_n):
        print(f"URI: {uri} - Total occurrences: {count}")

    print(f"\n=== Top {top_n} URIs by File Occurrence ===")
    for uri, count in file_occur.most_common(top_n):
        print(f"URI: {uri} - Found in {count} files")

def save_results(total_freq, file_occur, output_file):
    """Save results to a CSV file"""
    if not total_freq:
        print("No data to save.")
        return

    # Convert counters to DataFrames
    freq_df = pd.DataFrame(total_freq.items(), columns=['URI', 'Total_Frequency'])
    occur_df = pd.DataFrame(file_occur.items(), columns=['URI', 'File_Occurrences'])

    # Merge the two DataFrames on URI
    result_df = pd.merge(freq_df, occur_df, on='URI')

    # Sort by total frequency, descending
    result_df = result_df.sort_values('Total_Frequency', ascending=False)

    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Saved results to {output_file}")

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Analyze URI values in CSV files.')
    parser.add_argument('directory', help='Root directory containing CSV files')
    parser.add_argument('-o', '--output', default='uri_analysis_results.csv',
                        help='Output file name (default: uri_analysis_results.csv)')
    parser.add_argument('-n', '--top', type=int, default=20,
                        help='Number of top results to display (default: 20)')

    args = parser.parse_args()

    # Check if directory exists
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        sys.exit(1)

    # Run the analysis
    total_freq, file_occur = analyze_csv_files(args.directory)

    # Print results
    print_results(total_freq, file_occur, args.top)

    # Save results
    save_results(total_freq, file_occur, args.output)

if __name__ == "__main__":
    main()
