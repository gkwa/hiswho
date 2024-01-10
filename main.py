import csv
import os

import toolz


def delete_lines_until_header(file_path, header):
    with open(file_path, "r") as f:
        lines = f.readlines()

    # Find the index of the line containing the header
    header_index = next((i for i, line in enumerate(lines) if header in line), None)

    if header_index is not None:
        # Write the lines after the header to a temporary file
        temp_file_path = f"{file_path}.temp"
        with open(temp_file_path, "w") as temp_file:
            temp_file.writelines(lines[header_index:])

        # Replace the original file with the temporary file
        os.replace(temp_file_path, file_path)


def assert_header(file_path, header):
    print(f"assert_header: {file_path}")
    with open(file_path, "r") as f:
        csv_reader = csv.reader(f)
        file_header = next(csv_reader)

    assert header in file_header, f"Header '{header}' not found in {file_path}"


def update_header(file_path, old_header, new_header):
    with open(file_path, "r") as f:
        lines = f.readlines()

    # Find and replace the old header with the new header
    updated_lines = [line.replace(old_header, new_header) for line in lines]

    with open(file_path, "w") as f:
        f.writelines(updated_lines)


def process_data_files(original_file_paths, header_to_assert, old_header, new_header):
    for file_path in original_file_paths:
        toolz.pipe(
            file_path,
            lambda path: delete_lines_until_header(path, header_to_assert),
            lambda path: assert_header(path, header_to_assert),
            lambda path: update_header(path, old_header, new_header),
        )


old_header = "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"
new_header = "type,date,start_time,end_time,import_kwh,export_kwh,notes"
header_to_assert = old_header
original_file_paths = [
    "/Users/mtm/pdev/taylormonacelli/eachload/data/scl_electric_usage_interval_data_2280076854_1_2023-07-11_to_2023-09-06.csv"
]

process_data_files(original_file_paths, header_to_assert, old_header, new_header)
