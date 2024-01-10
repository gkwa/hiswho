import csv
import io
import pathlib

import pandas


def report_completion(
    content: io.StringIO, original_path: str, processed_path: str
) -> None:
    path1 = pathlib.Path(original_path)
    path2 = pathlib.Path(processed_path)
    print(f"Processed {path1.name} into {path2.name}")

    return content


def delete_processed_file(file_path: str) -> None:
    pathlib.Path(file_path).unlink(missing_ok=True)


def read_file(file_path: str) -> io.StringIO:
    with open(file_path, "r") as file:
        return io.StringIO(file.read())


def write_file(content: io.StringIO, file_path: str) -> io.StringIO:
    content.seek(0)
    with open(file_path, "w") as file:
        file.write(content.getvalue())

    return content


def append_notes_column(content: io.StringIO) -> io.StringIO:
    """transform this:
    type,start_time,end_time,import_kwh,export_kwh,notes
    Electric usage,1689033600,1689034440,0.01,0.00
    Electric usage,1689033600,1689034440,0.01,0.00,some note recarding this row

    into this:
    type,start_time,end_time,import_kwh,export_kwh,notes
    Electric usage,1689033600,1689034440,0.01,0.00,
    Electric usage,1689033600,1689034440,0.01,0.00,some note recarding this row

    which is the same, but with an extra comma at the end for records
    without notes.
    """
    content.seek(0)
    csv_reader = csv.reader(content)

    new_content = io.StringIO()
    writer = csv.writer(new_content)

    for row in csv_reader:
        if len(row) == 6:
            row.append("")
        writer.writerow(row)

    return new_content


def delete_lines_until_header(content: io.StringIO, header: str) -> io.StringIO:
    content.seek(0)
    content_str = content.getvalue()

    lines = content_str.split("\n")
    for i, line in enumerate(lines):
        if header in line:
            new_content = "\n".join(lines[i:])
            return io.StringIO(new_content)

    # Header not found, return the original StringIO object
    return content


def assert_column_headers(content: io.StringIO, header: str) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)
    actual_header = next(csv_reader)
    assert actual_header == header.split(","), "Header does not exist."
    return content


def update_header(content: io.StringIO, new_header: str) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    data = list(csv_reader)

    data[0] = new_header.split(",")

    new_content = io.StringIO()
    csv_writer = csv.writer(new_content)
    csv_writer.writerows(data)

    return new_content


def add_epoch_timestamps(content: io.StringIO) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))
    df["start_time"] = (
        pandas.to_datetime(df["date"] + " " + df["start_time"]).astype(int) // 10**9
    )
    df["end_time"] = (
        pandas.to_datetime(df["date"] + " " + df["end_time"]).astype(int) // 10**9
    )

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer


def remove_date_column(content: io.StringIO) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))
    df_dropped = df.drop("date", axis=1)

    buffer = io.StringIO()
    df_dropped.to_csv(buffer, index=False)
    return buffer


def process_file(original_path: str, processed_path: str) -> io.StringIO:
    delete_processed_file(processed_path)
    content = read_file(original_path)
    content = assert_column_headers(content, header1)
    content = delete_lines_until_header(content, header1)
    content = update_header(content, header2)
    content = append_notes_column(content)
    content = add_epoch_timestamps(content)
    content = remove_date_column(content)
    content = assert_column_headers(content, header3)
    content = write_file(content, processed_path)
    content = report_completion(content, original_path, processed_path)

    return content


original_file_path = "/Users/mtm/pdev/taylormonacelli/eachload/data/scl_electric_usage_interval_data_2280076854_1_2023-07-11_to_2023-09-06.csv"
processed_file_path = f"{original_file_path}.processed.csv"

header1 = "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"
header2 = "type,date,start_time,end_time,import_kwh,export_kwh,notes"
header3 = "type,start_time,end_time,import_kwh,export_kwh,notes"

process_file(original_file_path, processed_file_path)
