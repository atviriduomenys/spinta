import csv


def process_csv(file_path):
    # Read all lines from the CSV file
    with open(file_path, "r", newline="") as infile:
        reader = csv.reader(infile)
        all_rows = list(reader)

    # Get header row
    header = all_rows[0]

    # Find index of 'type' and 'ref' columns
    try:
        type_index = header.index("type")
        ref_index = header.index("ref")
        property_index = header.index("property")
    except ValueError as e:
        print(f"Error: Required column not found: {e}")
        return 0

    # Process the rows
    modified_count = 0
    rows_to_keep = []
    skip_property = None

    # Add the header row
    rows_to_keep.append(all_rows[0])

    i = 1  # Start from the first row after the header
    while i < len(all_rows):
        row = all_rows[i]

        # Skip rows if we have a property to skip
        if skip_property and i < len(all_rows):
            current_property = row[property_index] if property_index < len(row) else ""

            # If it starts with the property we're skipping (property_name.)
            if current_property.startswith(f"{skip_property}."):
                i += 1
                continue
            else:
                # Found a different property, stop skipping
                skip_property = None

        # Check if row has enough columns
        if len(row) > type_index and len(row) > ref_index:
            # Check if type starts with 'ref' or 'backref'
            if row[type_index] and (
                row[type_index].strip().startswith("ref") or row[type_index].strip().startswith("backref")
            ):
                # Store the property name for skipping related properties
                if property_index < len(row) and row[property_index]:
                    skip_property = row[property_index].strip()

                # Change type to 'str'
                row[type_index] = "string"

                # Clear ref column
                if ref_index < len(row):
                    row[ref_index] = ""

                modified_count += 1

        # Add the row to the ones we're keeping
        rows_to_keep.append(row)
        i += 1

    # Write the filtered rows back to the CSV file
    with open(file_path, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(rows_to_keep)

    return modified_count


# Usage
if __name__ == "__main__":
    file_path = "manifest.csv"  # Path to your CSV file

    num_modified = process_csv(file_path)
    print(f"Processing complete. Modified {num_modified} rows.")
