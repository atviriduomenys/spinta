import csv
import os


def remove_dot_refs(file_path):
    # Read all lines from the CSV file
    with open(file_path, "r", newline="") as infile:
        reader = csv.reader(infile)
        all_rows = list(reader)

    # Get header row
    header = all_rows[0]

    # Find index of 'ref' column
    try:
        ref_index = header.index("ref")
    except ValueError as e:
        print(f"Error: Required column 'ref' not found: {e}")
        return 0

    # Process each row
    modified_count = 0

    for i in range(1, len(all_rows)):  # Skip the header row
        row = all_rows[i]

        # Check if row has enough columns and the ref column has a value
        if len(row) > ref_index and row[ref_index]:
            original_value = row[ref_index]

            # Split by comma and process each value
            values = [val.strip() for val in original_value.split(",")]
            filtered_values = [val for val in values if "." not in val]

            # Join the filtered values back together
            new_value = ", ".join(filtered_values)

            # Update the row if there was a change
            if new_value != original_value:
                row[ref_index] = new_value
                modified_count += 1
                all_rows[i] = row

    # Write the modified rows back to the CSV file
    with open(file_path, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(all_rows)

    return modified_count


# Usage
if __name__ == "__main__":
    file_path = "manifest.csv"  # Path to your CSV file

    num_modified = remove_dot_refs(file_path)
    print(f"Processing complete. Modified {num_modified} ref cells by removing values with dots.")
