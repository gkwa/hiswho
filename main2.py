import csv

input_file_path = 'data.csv'
output_file_path = 'normalized_data.csv'

with open(input_file_path, 'r', newline='') as input_file:
    # Read CSV data into a list of dictionaries
    data = list(csv.DictReader(input_file))

    # Determine the maximum number of fields
    max_fields = max(len(row) for row in data)

    # Add a placeholder for 'NOTES' field in rows with fewer fields
    for row in data:
        row['NOTES'] = row.get('NOTES', '')

with open(output_file_path, 'w', newline='') as output_file:
    fieldnames = data[0].keys()
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

print(f"Normalization complete. Normalized data written to {output_file_path}.")
